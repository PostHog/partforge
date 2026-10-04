-- PostHog 542f8dc033d2850bfa2b3094be07716475f0516c (PostHog/posthog#111330): all 45 executable UDFs.
-- Empty funnels return no conversions; debug functions echo valid JSON.
WITH
    toUInt8(2) AS steps,
    toUInt8(1) AS from_step,
    toUInt64(1000) AS window,
    CAST([] AS Array(Nullable(String))) AS empty_1,
    CAST([] AS Array(Int8)) AS empty_2,
    CAST([] AS Array(Tuple(Nullable(Float64), UUID, Nullable(String), Array(Int8)))) AS empty_3,
    CAST([] AS Array(UInt64)) AS empty_4,
    CAST([] AS Array(Tuple(Nullable(Float64), UUID, UInt64, Array(Int8)))) AS empty_5,
    CAST([] AS Array(Array(String))) AS empty_6,
    CAST([] AS Array(Tuple(Nullable(Float64), UUID, Array(String), Array(Int8)))) AS empty_7,
    CAST([] AS Array(Tuple(Nullable(Float64), UInt64, UUID, Nullable(String), Array(Int8)))) AS empty_8,
    CAST([] AS Array(Tuple(Nullable(Float64), UInt64, UUID, Array(String), Array(Int8)))) AS empty_9,
    CAST([] AS Array(Tuple(Nullable(Float64), UInt64, UUID, UInt64, Array(Int8)))) AS empty_10,
    CAST([] AS Array(String)) AS empty_11
SELECT
    throwIf(notEmpty(aggregate_funnel(steps, window, 'first_touch', 'ordered', empty_1, empty_2, empty_3)), 'aggregate_funnel failed'),
    throwIf(notEmpty(aggregate_funnel_cohort(steps, window, 'first_touch', 'ordered', empty_4, empty_2, empty_5)), 'aggregate_funnel_cohort failed'),
    throwIf(notEmpty(aggregate_funnel_array(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_7)), 'aggregate_funnel_array failed'),
    throwIf(notEmpty(aggregate_funnel_trends(from_step, steps, steps, window, 'first_touch', 'ordered', empty_1, empty_8)), 'aggregate_funnel_trends failed'),
    throwIf(notEmpty(aggregate_funnel_array_trends(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)), 'aggregate_funnel_array_trends failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_trends(from_step, steps, steps, window, 'first_touch', 'ordered', empty_4, empty_10)), 'aggregate_funnel_cohort_trends failed'),
    throwIf(notEmpty(aggregate_funnel_json(steps, window, 'first_touch', 'ordered', empty_1, empty_2, empty_3)), 'aggregate_funnel_json failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_json(steps, window, 'first_touch', 'ordered', empty_4, empty_2, empty_5)), 'aggregate_funnel_cohort_json failed'),
    throwIf(notEmpty(aggregate_funnel_array_json(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_7)), 'aggregate_funnel_array_json failed'),
    throwIf(notEmpty(aggregate_funnel_trends_json(from_step, steps, steps, window, 'first_touch', 'ordered', empty_1, empty_8)), 'aggregate_funnel_trends_json failed'),
    throwIf(notEmpty(aggregate_funnel_array_trends_json(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)), 'aggregate_funnel_array_trends_json failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_trends_json(from_step, steps, steps, window, 'first_touch', 'ordered', empty_4, empty_10)), 'aggregate_funnel_cohort_trends_json failed'),
    throwIf(isValidJSON(aggregate_funnel_test(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_3)) = 0, 'aggregate_funnel_test failed'),
    throwIf(isValidJSON(aggregate_funnel_array_trends_test(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)) = 0, 'aggregate_funnel_array_trends_test failed'),
    throwIf(JSONDropKeys(['a'])('{"a":1,"b":2}') != '{"b":2}', 'JSONDropKeys failed'),
    throwIf(JSONCleanPostHogEvent('{"$set":{"score":7},"plan":null,"custom":"kept"}', '{"email":null}') != ('{"custom":"kept"}', '{"$set":{"score":7}}', '{}', ['plan'], [], ['email']), 'JSONCleanPostHogEvent failed'),
    throwIf(JSONCleanPostHogEvent('{"$sent_at":"2026-01-01","$sdk_debug_current_session_duration":5,"$sdk_debug_retry":1}', '{}') != ('{"$sent_at":"2026-01-01","$sdk_debug_current_session_duration":5}', '{"$sdk_debug_retry":1}', '{}', [], [], []), 'JSONCleanPostHogEvent permanent keys failed'),
    throwIf(JSONCleanPostHogEventProperties('{"a":1,"b":null}') != '{"a":1}', 'JSONCleanPostHogEventProperties failed'),
    throwIf(JSONCleanPostHogPersonProperties('{"a":1,"b":null}') != '{"a":1}', 'JSONCleanPostHogPersonProperties failed'),
    throwIf(JSONCleanPostHogTemporaryProperties('{"$set":{"a":1},"b":2}') != '{"$set":{"a":1}}', 'JSONCleanPostHogTemporaryProperties failed'),
    throwIf(JSONStripEmptyStringsAndNulls('{"a":1,"b":null,"c":""}') != '{"a":1}', 'JSONStripEmptyStringsAndNulls failed'),
    throwIf(decompress(unhex('170000006068656c6c6f200600d06f2068656c6c6f2068656c6c6f'), 'LZ4SizePrefixed') != 'hello hello hello hello', 'decompress failed'),
    throwIf(JSONDropKeysPool('{"a":1,"b":2}', ['a']) != '{"b":2}', 'JSONDropKeysPool failed'),
    throwIf(notEmpty(aggregate_funnel_v11(steps, window, 'first_touch', 'ordered', empty_1, empty_2, empty_3)), 'aggregate_funnel_v11 failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_v11(steps, window, 'first_touch', 'ordered', empty_4, empty_2, empty_5)), 'aggregate_funnel_cohort_v11 failed'),
    throwIf(notEmpty(aggregate_funnel_array_v11(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_7)), 'aggregate_funnel_array_v11 failed'),
    throwIf(isValidJSON(aggregate_funnel_test_v11(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_3)) = 0, 'aggregate_funnel_test_v11 failed'),
    throwIf(notEmpty(aggregate_funnel_trends_v11(from_step, steps, steps, window, 'first_touch', 'ordered', empty_1, empty_8)), 'aggregate_funnel_trends_v11 failed'),
    throwIf(notEmpty(aggregate_funnel_array_trends_v11(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)), 'aggregate_funnel_array_trends_v11 failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_trends_v11(from_step, steps, steps, window, 'first_touch', 'ordered', empty_4, empty_10)), 'aggregate_funnel_cohort_trends_v11 failed'),
    throwIf(isValidJSON(aggregate_funnel_array_trends_test_v11(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)) = 0, 'aggregate_funnel_array_trends_test_v11 failed'),
    throwIf(notEmpty(aggregate_funnel_v12(steps, window, 'first_touch', 'ordered', empty_1, empty_2, empty_3)), 'aggregate_funnel_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_v12(steps, window, 'first_touch', 'ordered', empty_4, empty_2, empty_5)), 'aggregate_funnel_cohort_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_array_v12(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_7)), 'aggregate_funnel_array_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_trends_v12(from_step, steps, steps, window, 'first_touch', 'ordered', empty_1, empty_8)), 'aggregate_funnel_trends_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_array_trends_v12(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)), 'aggregate_funnel_array_trends_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_trends_v12(from_step, steps, steps, window, 'first_touch', 'ordered', empty_4, empty_10)), 'aggregate_funnel_cohort_trends_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_json_v12(steps, window, 'first_touch', 'ordered', empty_1, empty_2, empty_3)), 'aggregate_funnel_json_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_json_v12(steps, window, 'first_touch', 'ordered', empty_4, empty_2, empty_5)), 'aggregate_funnel_cohort_json_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_array_json_v12(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_7)), 'aggregate_funnel_array_json_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_trends_json_v12(from_step, steps, steps, window, 'first_touch', 'ordered', empty_1, empty_8)), 'aggregate_funnel_trends_json_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_array_trends_json_v12(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)), 'aggregate_funnel_array_trends_json_v12 failed'),
    throwIf(notEmpty(aggregate_funnel_cohort_trends_json_v12(from_step, steps, steps, window, 'first_touch', 'ordered', empty_4, empty_10)), 'aggregate_funnel_cohort_trends_json_v12 failed'),
    throwIf(isValidJSON(aggregate_funnel_test_v12(steps, window, 'first_touch', 'ordered', empty_6, empty_2, empty_3)) = 0, 'aggregate_funnel_test_v12 failed'),
    throwIf(isValidJSON(aggregate_funnel_array_trends_test_v12(from_step, steps, steps, window, 'first_touch', 'ordered', empty_6, empty_9)) = 0, 'aggregate_funnel_array_trends_test_v12 failed'),
    throwIf(JSONDropKeys_v12(['a'])('{"a":1,"b":2}') != '{"b":2}', 'JSONDropKeys_v12 failed');

-- Reuse the new pools across chunks, with keys and cleaned values changing per row.
SELECT throwIf(countIf(
    JSONDropKeysPool(concat('{"a":', toString(number), ',"b":', toString(number), '}'), if(number % 2 = 0, ['a'], ['b']))
        != concat('{"', if(number % 2 = 0, 'b', 'a'), '":', toString(number), '}')
) != 0, 'JSONDropKeysPool chunk reuse failed')
FROM numbers(257)
SETTINGS max_block_size = 2, max_threads = 1;

SELECT throwIf(countIf(
    cleaned.properties != concat('{"id":', toString(number), '}')
    OR cleaned.temporary_properties != concat('{"$set":{"id":', toString(number), '}}')
    OR cleaned.person_properties != concat('{"id":', toString(number), '}')
    OR cleaned.properties_null_keys != ['missing']
    OR cleaned.temporary_properties_null_keys != []
    OR cleaned.person_properties_null_keys != ['missing']
) != 0, 'JSONCleanPostHogEvent chunk reuse failed')
FROM
(
    SELECT number, JSONCleanPostHogEvent(
        concat('{"id":', toString(number), ',"missing":null,"$set":{"id":', toString(number), '}}'),
        concat('{"id":', toString(number), ',"missing":null}')
    ) AS cleaned
    FROM numbers(257)
)
SETTINGS max_block_size = 2, max_threads = 1;
