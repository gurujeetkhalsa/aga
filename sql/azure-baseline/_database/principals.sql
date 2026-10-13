-- See principals.json for exact original identity/authentication metadata. Credentials are supplied separately.
CREATE USER [aga-dbrepl] FROM EXTERNAL PROVIDER;
CREATE USER [robert.qi@usgo.org] FROM EXTERNAL PROVIDER;
-- Provision [robertqisql] using DATABASE authentication; never commit its password.
