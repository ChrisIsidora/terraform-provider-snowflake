package snowflake_test

import (
	"testing"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
)

func TestAllSchemaGrant(t *testing.T) {
	r := require.New(t)
	fvg := snowflake.AllSchemaGrant("test_db")
	r.Equal(fvg.Name(), "test_db")

	s := fvg.Role("bob").Grant("USAGE", false)
	r.Equal(`GRANT USAGE ON ALL SCHEMAS IN DATABASE "test_db" TO ROLE "bob"`, s)

	revoke := fvg.Role("bob").Revoke("USAGE")
	r.Equal([]string{`REVOKE USAGE ON ALL SCHEMAS IN DATABASE "test_db" FROM ROLE "bob"`}, revoke)
}

func TestAllTableGrant(t *testing.T) {
	r := require.New(t)
	fvg := snowflake.AllTableGrant("test_db", "PUBLIC")
	r.Equal(fvg.Name(), "PUBLIC")

	s := fvg.Role("bob").Grant("USAGE", false)
	r.Equal(`GRANT USAGE ON ALL TABLES IN SCHEMA "test_db"."PUBLIC" TO ROLE "bob"`, s)

	revoke := fvg.Role("bob").Revoke("USAGE")
	r.Equal([]string{`REVOKE USAGE ON ALL TABLES IN SCHEMA "test_db"."PUBLIC" FROM ROLE "bob"`}, revoke)

	b := require.New(t)
	fvgd := snowflake.AllTableGrant("test_db", "")
	b.Equal(fvgd.Name(), "test_db")

	s = fvgd.Role("bob").Grant("USAGE", false)
	b.Equal(`GRANT USAGE ON ALL TABLES IN DATABASE "test_db" TO ROLE "bob"`, s)

	revoke = fvgd.Role("bob").Revoke("USAGE")
	b.Equal([]string{`REVOKE USAGE ON ALL TABLES IN DATABASE "test_db" FROM ROLE "bob"`}, revoke)
}

func TestAllMaterializedViewGrant(t *testing.T) {
	r := require.New(t)
	fvg := snowflake.AllMaterializedViewGrant("test_db", "PUBLIC")
	r.Equal(fvg.Name(), "PUBLIC")

	s := fvg.Role("bob").Grant("SELECT", false)
	r.Equal(`GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA "test_db"."PUBLIC" TO ROLE "bob"`, s)

	revoke := fvg.Role("bob").Revoke("SELECT")
	r.Equal([]string{`REVOKE SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA "test_db"."PUBLIC" FROM ROLE "bob"`}, revoke)

	b := require.New(t)
	fvgd := snowflake.AllMaterializedViewGrant("test_db", "")
	b.Equal(fvgd.Name(), "test_db")

	s = fvgd.Role("bob").Grant("SELECT", false)
	b.Equal(`GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE "test_db" TO ROLE "bob"`, s)

	revoke = fvgd.Role("bob").Revoke("SELECT")
	b.Equal([]string{`REVOKE SELECT ON ALL MATERIALIZED VIEWS IN DATABASE "test_db" FROM ROLE "bob"`}, revoke)
}

func TestAllViewGrant(t *testing.T) {
	r := require.New(t)
	fvg := snowflake.AllViewGrant("test_db", "PUBLIC")
	r.Equal(fvg.Name(), "PUBLIC")

	s := fvg.Role("bob").Grant("USAGE", false)
	r.Equal(`GRANT USAGE ON ALL VIEWS IN SCHEMA "test_db"."PUBLIC" TO ROLE "bob"`, s)

	revoke := fvg.Role("bob").Revoke("USAGE")
	r.Equal([]string{`REVOKE USAGE ON ALL VIEWS IN SCHEMA "test_db"."PUBLIC" FROM ROLE "bob"`}, revoke)

	b := require.New(t)
	fvgd := snowflake.AllViewGrant("test_db", "")
	b.Equal(fvgd.Name(), "test_db")

	s = fvgd.Role("bob").Grant("USAGE", false)
	b.Equal(`GRANT USAGE ON ALL VIEWS IN DATABASE "test_db" TO ROLE "bob"`, s)

	revoke = fvgd.Role("bob").Revoke("USAGE")
	b.Equal([]string{`REVOKE USAGE ON ALL VIEWS IN DATABASE "test_db" FROM ROLE "bob"`}, revoke)
}

func TestAllStageGrant(t *testing.T) {
	r := require.New(t)
	fvg := snowflake.AllStageGrant("test_db", "PUBLIC")
	r.Equal(fvg.Name(), "PUBLIC")

	s := fvg.Role("bob").Grant("USAGE", false)
	r.Equal(`GRANT USAGE ON ALL STAGES IN SCHEMA "test_db"."PUBLIC" TO ROLE "bob"`, s)

	revoke := fvg.Role("bob").Revoke("USAGE")
	r.Equal([]string{`REVOKE USAGE ON ALL STAGES IN SCHEMA "test_db"."PUBLIC" FROM ROLE "bob"`}, revoke)

	b := require.New(t)
	fvgd := snowflake.AllStageGrant("test_db", "")
	b.Equal(fvgd.Name(), "test_db")

	s = fvgd.Role("bob").Grant("USAGE", false)
	b.Equal(`GRANT USAGE ON ALL STAGES IN DATABASE "test_db" TO ROLE "bob"`, s)

	revoke = fvgd.Role("bob").Revoke("USAGE")
	b.Equal([]string{`REVOKE USAGE ON ALL STAGES IN DATABASE "test_db" FROM ROLE "bob"`}, revoke)
}

func TestAllExternalTableGrant(t *testing.T) {
	r := require.New(t)
	fvg := snowflake.AllExternalTableGrant("test_db", "PUBLIC")
	r.Equal(fvg.Name(), "PUBLIC")

	s := fvg.Role("bob").Grant("SELECT", false)
	r.Equal(`GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA "test_db"."PUBLIC" TO ROLE "bob"`, s)

	revoke := fvg.Role("bob").Revoke("SELECT")
	r.Equal([]string{`REVOKE SELECT ON ALL EXTERNAL TABLES IN SCHEMA "test_db"."PUBLIC" FROM ROLE "bob"`}, revoke)

	b := require.New(t)
	fvgd := snowflake.AllExternalTableGrant("test_db", "")
	b.Equal(fvgd.Name(), "test_db")

	s = fvgd.Role("bob").Grant("SELECT", false)
	b.Equal(`GRANT SELECT ON ALL EXTERNAL TABLES IN DATABASE "test_db" TO ROLE "bob"`, s)

	revoke = fvgd.Role("bob").Revoke("SELECT")
	b.Equal([]string{`REVOKE SELECT ON ALL EXTERNAL TABLES IN DATABASE "test_db" FROM ROLE "bob"`}, revoke)
}

func TestAllFileFormatGrant(t *testing.T) {
	r := require.New(t)
	fvg := snowflake.AllFileFormatGrant("test_db", "PUBLIC")
	r.Equal(fvg.Name(), "PUBLIC")

	s := fvg.Role("bob").Grant("USAGE", false)
	r.Equal(`GRANT USAGE ON ALL FILE FORMATS IN SCHEMA "test_db"."PUBLIC" TO ROLE "bob"`, s)

	revoke := fvg.Role("bob").Revoke("USAGE")
	r.Equal([]string{`REVOKE USAGE ON ALL FILE FORMATS IN SCHEMA "test_db"."PUBLIC" FROM ROLE "bob"`}, revoke)

	b := require.New(t)
	fvgd := snowflake.AllFileFormatGrant("test_db", "")
	b.Equal(fvgd.Name(), "test_db")

	s = fvgd.Role("bob").Grant("USAGE", false)
	b.Equal(`GRANT USAGE ON ALL FILE FORMATS IN DATABASE "test_db" TO ROLE "bob"`, s)

	revoke = fvgd.Role("bob").Revoke("USAGE")
	b.Equal([]string{`REVOKE USAGE ON ALL FILE FORMATS IN DATABASE "test_db" FROM ROLE "bob"`}, revoke)
}
