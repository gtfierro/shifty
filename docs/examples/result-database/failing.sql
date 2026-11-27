SELECT vav.*
FROM [ex:VavShape] AS vav
JOIN _shifty_result_validation USING (result_id)
WHERE conforms = 0;
