AUDIT (
  name not_null_wiki,
);

SELECT *
FROM @this_model
WHERE wiki IS NULL OR wiki = ''
