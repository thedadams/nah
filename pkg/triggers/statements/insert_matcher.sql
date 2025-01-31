INSERT INTO placeholder_matchers(
                                 target_api_version,
                                 target_kind,
                                 target_key,
                                 namespace,
                                 name,
                                 label_selectors,
                                 field_selectors)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT DO NOTHING;