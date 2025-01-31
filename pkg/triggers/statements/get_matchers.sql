SELECT
    target_api_version,
    target_kind,
    target_key,
    namespace,
    name,
    label_selectors,
    field_selectors
FROM placeholder_matchers
WHERE
    (namespace = $1 OR $1 IS NULL) AND
    (name = $2 OR $2 IS NULL);