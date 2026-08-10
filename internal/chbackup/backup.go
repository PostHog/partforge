package chbackup

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

type Layer struct {
	Bucket    string
	Prefix    string
	IndexPath string
}

type Segment struct {
	Bucket string
	Key    string
}

type File struct {
	Name     string
	Size     uint64
	Segments []Segment
}

type Part struct {
	Name  string
	Files []File
	Bytes uint64
}

type Header struct {
	UUID       string
	BaseBackup string
	BaseUUID   string
}

type Info struct {
	Header    Header
	Metadata  File
	PartCount int
}

type Plan struct {
	layers       []Layer
	database     string
	table        string
	baseResolved map[contentKey][]Segment
	info         Info
}

type rawFile struct {
	Name         string  `xml:"name"`
	Size         uint64  `xml:"size"`
	Checksum     string  `xml:"checksum"`
	DataFile     string  `xml:"data_file"`
	UseBase      bool    `xml:"use_base"`
	BaseSizeXML  *uint64 `xml:"base_size"`
	BaseChecksum string  `xml:"base_checksum"`
	ObjectKey    string  `xml:"object_key"`
	Encrypted    bool    `xml:"encrypted_by_disk"`
	BaseSize     uint64  `xml:"-"`
}

type rawPart struct {
	Name  string
	Files []rawFile
	Bytes uint64
}

type contentKey struct {
	Size     uint64
	Checksum string
}

func ReadHeader(r io.Reader) (Header, error) {
	decoder := xml.NewDecoder(r)
	var version string
	var header Header
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return Header{}, fmt.Errorf("file is not ClickHouse backup metadata")
		}
		if err != nil {
			return Header{}, fmt.Errorf("parse ClickHouse backup metadata: %w", err)
		}
		elem, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch elem.Name.Local {
		case "version":
			if err := decoder.DecodeElement(&version, &elem); err != nil {
				return Header{}, err
			}
		case "uuid":
			if err := decoder.DecodeElement(&header.UUID, &elem); err != nil {
				return Header{}, err
			}
		case "base_backup":
			if err := decoder.DecodeElement(&header.BaseBackup, &elem); err != nil {
				return Header{}, err
			}
		case "base_backup_uuid":
			if err := decoder.DecodeElement(&header.BaseUUID, &elem); err != nil {
				return Header{}, err
			}
		case "contents":
			if strings.TrimSpace(version) != "1" {
				return Header{}, fmt.Errorf("unsupported ClickHouse backup version %q", strings.TrimSpace(version))
			}
			header.BaseBackup = strings.TrimSpace(header.BaseBackup)
			header.UUID = strings.TrimSpace(header.UUID)
			header.BaseUUID = strings.TrimSpace(header.BaseUUID)
			return header, nil
		}
	}
}

func Prepare(layers []Layer, database, table string) (*Plan, error) {
	if len(layers) == 0 {
		return nil, fmt.Errorf("at least one ClickHouse backup layer is required")
	}
	if database == "" || table == "" {
		return nil, fmt.Errorf("database and table are required")
	}
	for i, layer := range layers {
		if layer.Bucket == "" || layer.Prefix == "" || layer.IndexPath == "" {
			return nil, fmt.Errorf("ClickHouse backup layer %d is incomplete", i)
		}
	}

	required := make(map[contentKey]struct{})
	index, err := os.Open(layers[0].IndexPath)
	if err != nil {
		return nil, err
	}
	header, metadata, partCount, scanErr := scanTable(index, database, table, func(part rawPart) error {
		for _, file := range part.Files {
			addBaseRequirement(required, file)
		}
		return nil
	})
	closeErr := index.Close()
	if scanErr != nil {
		return nil, scanErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	addBaseRequirement(required, metadata)

	matches := make([]map[contentKey]rawFile, len(layers))
	for level := 1; level < len(layers) && len(required) > 0; level++ {
		found, err := findContent(layers[level].IndexPath, required)
		if err != nil {
			return nil, fmt.Errorf("scan base backup layer %d: %w", level, err)
		}
		for key := range required {
			if _, ok := found[key]; !ok {
				return nil, fmt.Errorf("base backup layer %d does not contain file with size %d and checksum %s", level, key.Size, key.Checksum)
			}
		}
		matches[level] = found
		required = make(map[contentKey]struct{})
		for _, file := range found {
			addBaseRequirement(required, file)
		}
	}
	if len(required) > 0 {
		for key := range required {
			return nil, fmt.Errorf("backup chain ended before resolving file with size %d and checksum %s", key.Size, key.Checksum)
		}
	}

	resolved := make([]map[contentKey][]Segment, len(layers))
	for level := len(layers) - 1; level >= 1; level-- {
		resolved[level] = make(map[contentKey][]Segment, len(matches[level]))
		for key, file := range matches[level] {
			segments, err := resolveRawFile(file, level, layers, resolved)
			if err != nil {
				return nil, err
			}
			resolved[level][key] = segments
		}
	}

	plan := &Plan{layers: append([]Layer(nil), layers...), database: database, table: table}
	if len(resolved) > 1 {
		plan.baseResolved = resolved[1]
	}
	resolvedMetadata, err := plan.resolveHeadFile(metadata)
	if err != nil {
		return nil, fmt.Errorf("resolve table metadata: %w", err)
	}
	plan.info = Info{Header: header, Metadata: resolvedMetadata, PartCount: partCount}
	return plan, nil
}

func (p *Plan) Info() Info {
	return p.info
}

func (p *Plan) ScanParts(onPart func(Part) error) error {
	if onPart == nil {
		return fmt.Errorf("part callback is required")
	}
	index, err := os.Open(p.layers[0].IndexPath)
	if err != nil {
		return err
	}
	_, _, _, scanErr := scanTable(index, p.database, p.table, func(raw rawPart) error {
		part := Part{Name: raw.Name, Bytes: raw.Bytes, Files: make([]File, 0, len(raw.Files))}
		for _, file := range raw.Files {
			resolved, err := p.resolveHeadFile(file)
			if err != nil {
				return fmt.Errorf("resolve backup part %s file %s: %w", raw.Name, file.Name, err)
			}
			part.Files = append(part.Files, resolved)
		}
		return onPart(part)
	})
	closeErr := index.Close()
	if scanErr != nil {
		return scanErr
	}
	return closeErr
}

func (p *Plan) resolveHeadFile(file rawFile) (File, error) {
	resolved := []map[contentKey][]Segment{nil, p.baseResolved}
	segments, err := resolveRawFile(file, 0, p.layers, resolved)
	if err != nil {
		return File{}, err
	}
	return File{Name: file.Name, Size: file.Size, Segments: segments}, nil
}

func resolveRawFile(file rawFile, level int, layers []Layer, resolved []map[contentKey][]Segment) ([]Segment, error) {
	var segments []Segment
	if key, ok := file.baseKey(); ok {
		if level+1 >= len(resolved) {
			return nil, fmt.Errorf("file %s requires a missing base backup", file.Name)
		}
		base, found := resolved[level+1][key]
		if !found {
			return nil, fmt.Errorf("file %s base content with size %d and checksum %s is unresolved", file.Name, key.Size, key.Checksum)
		}
		segments = append(segments, base...)
	}
	if file.Size > file.BaseSize {
		if file.DataFile == "" {
			return nil, fmt.Errorf("file %s has current data but no data file", file.Name)
		}
		segments = append(segments, Segment{Bucket: layers[level].Bucket, Key: path.Join(layers[level].Prefix, file.DataFile)})
	}
	if file.Size > 0 && len(segments) == 0 {
		return nil, fmt.Errorf("file %s has no data source", file.Name)
	}
	return segments, nil
}

func addBaseRequirement(required map[contentKey]struct{}, file rawFile) {
	if key, ok := file.baseKey(); ok {
		required[key] = struct{}{}
	}
}

func (f rawFile) baseKey() (contentKey, bool) {
	if f.BaseSize == 0 {
		return contentKey{}, false
	}
	checksum := f.BaseChecksum
	if f.BaseSize == f.Size {
		checksum = f.Checksum
	}
	return contentKey{Size: f.BaseSize, Checksum: checksum}, true
}

func findContent(indexPath string, wanted map[contentKey]struct{}) (map[contentKey]rawFile, error) {
	index, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	found := make(map[contentKey]rawFile, len(wanted))
	_, scanErr := scanIndex(index, func(file rawFile) error {
		key := contentKey{Size: file.Size, Checksum: file.Checksum}
		if _, ok := wanted[key]; ok {
			if _, exists := found[key]; !exists {
				found[key] = file
			}
		}
		return nil
	})
	closeErr := index.Close()
	if scanErr != nil {
		return nil, scanErr
	}
	return found, closeErr
}

func scanTable(r io.Reader, database, table string, onPart func(rawPart) error) (Header, rawFile, int, error) {
	dataPrefix := path.Join("data", EscapeForFileName(database), EscapeForFileName(table)) + "/"
	metadataName := path.Join("metadata", EscapeForFileName(database), EscapeForFileName(table)+".sql")
	var metadata rawFile
	var current rawPart
	partCount := 0
	flush := func() error {
		if current.Name == "" {
			return nil
		}
		partCount++
		if onPart != nil {
			if err := onPart(current); err != nil {
				return fmt.Errorf("process backup part %s: %w", current.Name, err)
			}
		}
		current = rawPart{}
		return nil
	}

	header, err := scanIndex(r, func(file rawFile) error {
		if file.Name == metadataName {
			if err := flush(); err != nil {
				return err
			}
			metadata = file
			return nil
		}
		if !strings.HasPrefix(file.Name, dataPrefix) {
			return flush()
		}
		remainder := strings.TrimPrefix(file.Name, dataPrefix)
		partName, fileName, ok := strings.Cut(remainder, "/")
		if !ok || partName == "" || fileName == "" {
			return fmt.Errorf("invalid ClickHouse part file path %q", file.Name)
		}
		if partName == "mutations" {
			return flush()
		}
		if current.Name != partName {
			if err := flush(); err != nil {
				return err
			}
			current.Name = partName
		}
		if ^uint64(0)-current.Bytes < file.Size {
			return fmt.Errorf("ClickHouse part %s size overflows uint64", partName)
		}
		current.Bytes += file.Size
		file.Name = fileName
		current.Files = append(current.Files, file)
		return nil
	})
	if err != nil {
		return Header{}, rawFile{}, 0, err
	}
	if err := flush(); err != nil {
		return Header{}, rawFile{}, 0, err
	}
	if metadata.Name == "" || metadata.Size == 0 {
		return Header{}, rawFile{}, 0, fmt.Errorf("table %s.%s metadata not found in backup", database, table)
	}
	if partCount == 0 {
		return Header{}, rawFile{}, 0, fmt.Errorf("table %s.%s has no parts in backup", database, table)
	}
	return header, metadata, partCount, nil
}

func scanIndex(r io.Reader, onFile func(rawFile) error) (Header, error) {
	decoder := xml.NewDecoder(r)
	var version string
	var header Header
	var lastName string
	var sawConfig, sawContents, inContents bool
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Header{}, fmt.Errorf("parse ClickHouse backup metadata: %w", err)
		}
		switch elem := token.(type) {
		case xml.StartElement:
			switch {
			case elem.Name.Local == "config" && !sawConfig:
				sawConfig = true
			case elem.Name.Local == "contents" && !inContents:
				if strings.TrimSpace(version) != "1" {
					return Header{}, fmt.Errorf("unsupported ClickHouse backup version %q", strings.TrimSpace(version))
				}
				inContents = true
				sawContents = true
			case inContents && elem.Name.Local == "file":
				var file rawFile
				if err := decoder.DecodeElement(&file, &elem); err != nil {
					return Header{}, fmt.Errorf("parse ClickHouse backup file: %w", err)
				}
				if err := normalizeFile(&file); err != nil {
					return Header{}, err
				}
				if lastName != "" && file.Name <= lastName {
					return Header{}, fmt.Errorf("ClickHouse backup files are not strictly sorted: %q after %q", file.Name, lastName)
				}
				lastName = file.Name
				if err := onFile(file); err != nil {
					return Header{}, err
				}
			case !inContents && elem.Name.Local == "version":
				if err := decoder.DecodeElement(&version, &elem); err != nil {
					return Header{}, err
				}
			case !inContents && elem.Name.Local == "uuid":
				if err := decoder.DecodeElement(&header.UUID, &elem); err != nil {
					return Header{}, err
				}
			case !inContents && elem.Name.Local == "base_backup":
				if err := decoder.DecodeElement(&header.BaseBackup, &elem); err != nil {
					return Header{}, err
				}
			case !inContents && elem.Name.Local == "base_backup_uuid":
				if err := decoder.DecodeElement(&header.BaseUUID, &elem); err != nil {
					return Header{}, err
				}
			}
		case xml.EndElement:
			if elem.Name.Local == "contents" && inContents {
				inContents = false
			}
		}
	}
	if !sawConfig || !sawContents {
		return Header{}, fmt.Errorf("file is not ClickHouse backup metadata")
	}
	header.BaseBackup = strings.TrimSpace(header.BaseBackup)
	header.UUID = strings.TrimSpace(header.UUID)
	header.BaseUUID = strings.TrimSpace(header.BaseUUID)
	return header, nil
}

func normalizeFile(file *rawFile) error {
	if err := validatePath(file.Name); err != nil {
		return fmt.Errorf("invalid ClickHouse backup file name: %w", err)
	}
	if file.DataFile != "" {
		if err := validatePath(file.DataFile); err != nil {
			return fmt.Errorf("invalid ClickHouse backup data_file for %s: %w", file.Name, err)
		}
	}
	if file.ObjectKey != "" {
		return fmt.Errorf("lightweight ClickHouse backup entry %s is not supported", file.Name)
	}
	if file.Encrypted {
		return fmt.Errorf("encrypted ClickHouse backup entry %s is not supported", file.Name)
	}
	if file.Size == 0 {
		return nil
	}
	if file.Checksum == "" {
		return fmt.Errorf("ClickHouse backup entry %s is missing checksum", file.Name)
	}
	file.Checksum = strings.ToLower(file.Checksum)
	if file.BaseSizeXML != nil {
		file.BaseSize = *file.BaseSizeXML
	} else if file.UseBase {
		file.BaseSize = file.Size
	}
	if file.BaseSize > file.Size {
		return fmt.Errorf("ClickHouse backup entry %s base size exceeds file size", file.Name)
	}
	if file.BaseSize > 0 && file.BaseSize < file.Size {
		file.BaseChecksum = strings.ToLower(strings.TrimSpace(file.BaseChecksum))
		if file.BaseChecksum == "" {
			return fmt.Errorf("ClickHouse backup entry %s is missing base checksum", file.Name)
		}
	}
	if file.Size > file.BaseSize && file.DataFile == "" {
		file.DataFile = file.Name
	}
	return nil
}

func validatePath(name string) error {
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) || path.Clean(name) != name {
		return fmt.Errorf("unsafe path %q", name)
	}
	return nil
}

func EscapeForFileName(value string) string {
	const hex = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '_' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' {
			out.WriteByte(c)
			continue
		}
		out.WriteByte('%')
		out.WriteByte(hex[c>>4])
		out.WriteByte(hex[c&15])
	}
	return out.String()
}
