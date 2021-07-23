package snowflake

import (
	"fmt"
)

type allGrantType string
type allGrantTarget string

const (
	allSchemaType           allGrantType = "SCHEMA"
	allTableType            allGrantType = "TABLE"
	allViewType             allGrantType = "VIEW"
	allMaterializedViewType allGrantType = "MATERIALIZED VIEW"
	allStageType            allGrantType = "STAGE"
	allExternalTableType    allGrantType = "EXTERNAL TABLE"
	allFileFormatType       allGrantType = "FILE FORMAT"
	allFunctionType         allGrantType = "FUNCTION"
	allProcedureType        allGrantType = "PROCEDURE"
	allSequenceType         allGrantType = "SEQUENCE"
	allStreamType           allGrantType = "STREAM"
)

const (
	allSchemaTarget   allGrantTarget = "SCHEMA"
	allDatabaseTarget allGrantTarget = "DATABASE"
)

// AllGrantBuilder abstracts the creation of AllGrantExecutables
type AllGrantBuilder struct {
	name           string
	qualifiedName  string
	allGrantType   allGrantType
	allGrantTarget allGrantTarget
}

func getNameAndQualifiedNameAll(db, schema string) (string, string, allGrantTarget) {
	name := schema
	allTarget := allSchemaTarget
	qualifiedName := fmt.Sprintf(`"%v"."%v"`, db, schema)

	if schema == "" {
		name = db
		allTarget = allDatabaseTarget
		qualifiedName = fmt.Sprintf(`"%v"`, db)
	}

	return name, qualifiedName, allTarget
}

// Name returns the object name for this AllGrantBuilder
func (fgb *AllGrantBuilder) Name() string {
	return fgb.name
}

func (fgb *AllGrantBuilder) GrantType() string {
	return string(fgb.allGrantType)
}

// AllSchemaGrant returns a pointer to a AllGrantBuilder for a schema
func AllSchemaGrant(db string) GrantBuilder {
	return &AllGrantBuilder{
		name:           db,
		qualifiedName:  fmt.Sprintf(`"%v"`, db),
		allGrantType:   allSchemaType,
		allGrantTarget: allDatabaseTarget,
	}
}

// AllTableGrant returns a pointer to a AllGrantBuilder for a table
func AllTableGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allTableType,
		allGrantTarget: allTarget,
	}
}

// AllViewGrant returns a pointer to a AllGrantBuilder for a view
func AllViewGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allViewType,
		allGrantTarget: allTarget,
	}
}

// AllMaterializedViewGrant returns a pointer to a AllGrantBuilder for a view
func AllMaterializedViewGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allMaterializedViewType,
		allGrantTarget: allTarget,
	}
}

// AllStageGrant returns a pointer to a AllGrantBuilder for a table
func AllStageGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allStageType,
		allGrantTarget: allTarget,
	}
}

// AllExternalTableGrant returns a pointer to a AllGrantBuilder for a view
func AllExternalTableGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allExternalTableType,
		allGrantTarget: allTarget,
	}
}

// AllFileFormatGrant returns a pointer to a AllGrantBuilder for a view
func AllFileFormatGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allFileFormatType,
		allGrantTarget: allTarget,
	}
}

// AllFunctionGrant returns a pointer to a AllGrantBuilder for a view
func AllFunctionGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allFunctionType,
		allGrantTarget: allTarget,
	}
}

// AllProcedureGrant returns a pointer to a AllGrantBuilder for a view
func AllProcedureGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allProcedureType,
		allGrantTarget: allTarget,
	}
}

// AllSequenceGrant returns a pointer to a AllGrantBuilder for a view
func AllSequenceGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allSequenceType,
		allGrantTarget: allTarget,
	}
}

// AllStreamGrant returns a pointer to a AllGrantBuilder for a view
func AllStreamGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getNameAndQualifiedNameAll(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allStreamType,
		allGrantTarget: allTarget,
	}
}

// Show returns the SQL that will show all privileges on the grant
func (fgb *AllGrantBuilder) Show() string {
	return fmt.Sprintf(`SHOW FUTURE GRANTS IN %v %v`, fgb.allGrantTarget, fgb.qualifiedName)
}

// AllGrantExecutable abstracts the creation of SQL queries to build all grants for
// different all grant types.
type AllGrantExecutable struct {
	grantName      string
	granteeName    string
	allGrantType   allGrantType
	allGrantTarget allGrantTarget
}

// Role returns a pointer to a AllGrantExecutable for a role
func (fgb *AllGrantBuilder) Role(n string) GrantExecutable {
	return &AllGrantExecutable{
		granteeName:    n,
		grantName:      fgb.qualifiedName,
		allGrantType:   fgb.allGrantType,
		allGrantTarget: fgb.allGrantTarget,
	}
}

// Share is not implemented because all objects cannot be granted to shares.
func (gb *AllGrantBuilder) Share(n string) GrantExecutable {
	return nil
}

// Grant returns the SQL that will grant all privileges on the grant to the grantee
func (fge *AllGrantExecutable) Grant(p string, w bool) string {
	var template string
	if w {
		template = `GRANT %v ON FUTURE %vS IN %v %v TO ROLE "%v" WITH GRANT OPTION`
	} else {
		template = `GRANT %v ON FUTURE %vS IN %v %v TO ROLE "%v"`
	}
	return fmt.Sprintf(template,
		p, fge.allGrantType, fge.allGrantTarget, fge.grantName, fge.granteeName)
}

// Revoke returns the SQL that will revoke all privileges on the grant from the grantee
func (fge *AllGrantExecutable) Revoke(p string) []string {
	return []string{
		fmt.Sprintf(`REVOKE %v ON FUTURE %vS IN %v %v FROM ROLE "%v"`,
			p, fge.allGrantType, fge.allGrantTarget, fge.grantName, fge.granteeName),
	}
}

// Show returns the SQL that will show all all grants on the schema
func (fge *AllGrantExecutable) Show() string {
	return fmt.Sprintf(`SHOW FUTURE GRANTS IN %v %v`, fge.allGrantTarget, fge.grantName)
}
