SELECT
    NULL                AS "StreetPrefix",
    s.name              AS "StreetName",
    NULL                AS "BuildingNumber",
    c.name              AS "City",
    pc.code             AS "PostalCode",
    NULL                AS "Commune",
    NULL                AS "District",
    NULL                AS "Province"
FROM "primary".tb_address_property_mappings map
LEFT JOIN "primary".tb_cities c        ON c.id = map.city_id
LEFT JOIN "primary".tb_streets s       ON s.id = map.street_id
LEFT JOIN "primary".tb_postal_codes pc ON pc.id = map.postal_code_id
WHERE
    ( @City        = '' OR c.name ILIKE '%' || @City        || '%' )
    AND ( @StreetName   = '' OR s.name ILIKE '%' || @StreetName   || '%' )
    AND ( @PostalCode   = '' OR pc.code ILIKE '%' || @PostalCode  || '%' );

