SELECT vav.focus, vav.airflow, locations.room, locations.profile_label
FROM [ex:VavShape] AS vav
JOIN vav_locations AS locations USING (focus)
ORDER BY vav.focus, vav.airflow;
