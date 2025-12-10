SELECT
    street_prefix  AS "StreetPrefix",
    street_name    AS "StreetName",
    building_no    AS "BuildingNumber",
    city           AS "City",
    postal_code    AS "PostalCode",
    commune        AS "Commune",
    district       AS "District",
    province       AS "Province"
FROM addresses
WHERE
    street_name ILIKE @StreetName
    AND COALESCE(street_prefix, '') ILIKE @StreetPrefix
    AND COALESCE(building_no, '') = @BuildingNumber
    AND COALESCE(city, '') ILIKE @City
    AND COALESCE(postal_code, '') = @PostalCode;

