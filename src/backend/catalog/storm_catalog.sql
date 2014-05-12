CREATE VIEW storm_catalog.pg_roles AS
    SELECT *
      FROM pg_catalog.pg_roles
     WHERE rolname = current_user
        OR split_part(rolname, '@', 2) = current_database();

GRANT SELECT on storm_catalog.pg_roles TO PUBLIC;

REVOKE ALL on pg_catalog.pg_roles FROM public;

CREATE VIEW storm_catalog.pg_shdescription AS
    SELECT d.objoid, d.classoid, d.description
      FROM pg_catalog.pg_shdescription d, pg_catalog.pg_class c
     WHERE d.classoid = c.oid
       AND c.relname = 'pg_database'
       AND d.objoid = (SELECT oid FROM pg_database WHERE datname = current_database())
    UNION
    SELECT d.objoid, d.classoid, d.description
      FROM pg_catalog.pg_shdescription d, pg_catalog.pg_class c
     WHERE d.classoid = c.oid
       AND c.relname = 'pg_authid'
       AND d.objoid = (SELECT oid FROM storm_catalog.pg_roles WHERE rolname = current_user);

GRANT SELECT on storm_catalog.pg_shdescription TO PUBLIC;

REVOKE ALL on pg_catalog.pg_shdescription FROM public;

CREATE VIEW storm_catalog.pg_database AS
    SELECT tableoid, oid, datname, datdba, encoding, datcollate, datctype,
           datistemplate, datallowconn, datconnlimit, datlastsysoid,
           datfrozenxid, dattablespace, datacl
      FROM pg_catalog.pg_database
	 WHERE datallowconn AND (has_database_privilege(datname, 'CREATE') OR
           split_part(current_user, '@', 2) = datname);

GRANT SELECT on storm_catalog.pg_database TO PUBLIC;

REVOKE ALL on pg_catalog.pg_database FROM public;

CREATE VIEW storm_catalog.pg_db_role_setting AS
    SELECT setdatabase, setrole, setconfig
      FROM pg_catalog.pg_db_role_setting
     WHERE setdatabase = (SELECT oid FROM pg_database WHERE datname = current_database())
    UNION
    SELECT setdatabase, setrole, setconfig
      FROM pg_db_role_setting
     WHERE setrole = (SELECT oid FROM storm_catalog.pg_roles WHERE rolname = current_user);

GRANT SELECT on storm_catalog.pg_db_role_setting TO PUBLIC;

REVOKE ALL on pg_catalog.pg_db_role_setting FROM public;

CREATE VIEW storm_catalog.pg_tablespace AS
    SELECT oid, spcname, spcowner, ''::text as spclocation, ''::text as spcacl,
		''::text as spcoptions FROM pg_catalog.pg_tablespace;

GRANT SELECT on storm_catalog.pg_tablespace TO PUBLIC;

REVOKE ALL on pg_catalog.pg_tablespace FROM public;

CREATE VIEW storm_catalog.pg_auth_members AS
    SELECT roleid, member, grantor, admin_option
      FROM pg_catalog.pg_auth_members
     WHERE roleid = (SELECT oid FROM storm_catalog.pg_roles WHERE rolname = current_user)
    UNION
    SELECT roleid, member, grantor, admin_option
      FROM pg_catalog.pg_auth_members
     WHERE grantor = (SELECT oid FROM storm_catalog.pg_roles WHERE rolname = current_user);

GRANT SELECT on storm_catalog.pg_auth_members TO PUBLIC;

REVOKE ALL on pg_catalog.pg_auth_members FROM public;

CREATE VIEW storm_catalog.pg_shdepend AS
    SELECT dbid, classid, objid, objsubid, refclassid, refobjid, deptype
      FROM pg_catalog.pg_shdepend
     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database());

GRANT SELECT on storm_catalog.pg_shdepend TO PUBLIC;

REVOKE ALL on pg_catalog.pg_shdepend FROM public;

CREATE VIEW storm_catalog.pg_stat_database AS
    SELECT *
      FROM pg_catalog.pg_stat_database
     WHERE datid = (SELECT oid FROM pg_database WHERE datname = current_database());

GRANT SELECT on storm_catalog.pg_stat_database TO PUBLIC;

REVOKE ALL on pg_catalog.pg_stat_database FROM public;

CREATE VIEW storm_catalog.pg_stat_database_conflicts AS
    SELECT *
      FROM pg_catalog.pg_stat_database_conflicts
     WHERE datid = (SELECT oid FROM pg_database WHERE datname = current_database());

GRANT SELECT on storm_catalog.pg_stat_database_conflicts TO PUBLIC;

REVOKE ALL on pg_catalog.pg_stat_database_conflicts FROM public;


CREATE VIEW storm_catalog.pg_prepared_xacts AS
    SELECT *
      FROM pg_catalog.pg_prepared_xacts
     WHERE database = current_database();

GRANT SELECT on storm_catalog.pg_prepared_xacts TO PUBLIC;

REVOKE ALL on pg_catalog.pg_prepared_xacts FROM public;

CREATE VIEW storm_catalog.pg_user AS
    SELECT *
      FROM pg_catalog.pg_user
     WHERE usename = current_user
        OR split_part(usename, '@', 2) = current_database();

GRANT SELECT on storm_catalog.pg_user TO PUBLIC;

REVOKE ALL on pg_catalog.pg_user FROM public;

CREATE VIEW storm_catalog.pg_group AS
    SELECT *
      FROM pg_catalog.pg_group
     WHERE split_part(groname, '@', 2) = current_database();

GRANT SELECT on storm_catalog.pg_group TO PUBLIC;

REVOKE ALL on pg_catalog.pg_group FROM public;

CREATE VIEW storm_catalog.pg_shadow AS
    SELECT *
      FROM pg_catalog.pg_shadow
     WHERE usename = current_user
        OR split_part(usename, '@', 2) = current_database();

GRANT SELECT on storm_catalog.pg_shadow TO PUBLIC;

REVOKE ALL on pg_catalog.pg_shadow FROM public;

CREATE VIEW storm_catalog.pg_user_mappings AS
    SELECT *
      FROM pg_catalog.pg_user_mappings
     WHERE usename = current_user
        OR split_part(usename, '@', 2) = current_database();

GRANT SELECT on storm_catalog.pg_user_mappings TO PUBLIC;

REVOKE ALL on pg_catalog.pg_user_mappings FROM public;

REVOKE ALL on pg_catalog.pg_stat_bgwriter FROM public;

REVOKE ALL on pg_catalog.pg_seclabels FROM public;

REVOKE ALL on FUNCTION pg_catalog.pg_conf_load_time() FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_current_xlog_insert_location() FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_current_xlog_location() FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_is_in_recovery() FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_last_xlog_receive_location() FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_last_xlog_replay_location() FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_postmaster_start_time() FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_tablespace_databases(oid) FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_tablespace_size(oid) FROM PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_tablespace_size(name) FROM PUBLIC;

CREATE FUNCTION storm_catalog.pg_database_size(name) RETURNS bigint AS
$BODY$
BEGIN
  IF $1 = current_database() THEN
    return pg_catalog.pg_database_size($1);
  END IF;

  return 0;
END
$BODY$
LANGUAGE 'plpgsql' ;

GRANT EXECUTE on FUNCTION storm_catalog.pg_database_size(name) TO PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_database_size(name) FROM PUBLIC;

CREATE OR REPLACE FUNCTION storm_catalog.pg_database_size(oid) RETURNS bigint AS
$BODY$
DECLARE
  is_current_db boolean;
BEGIN
  SELECT $1 = oid
    INTO is_current_db
    FROM pg_catalog.pg_database
   WHERE datname = current_database();

  IF is_current_db THEN
    return pg_catalog.pg_database_size($1);
  END IF;

  return 0;
END
$BODY$
LANGUAGE 'plpgsql' ;

GRANT EXECUTE on FUNCTION storm_catalog.pg_database_size(oid) TO PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_database_size(oid) FROM PUBLIC;

CREATE FUNCTION storm_catalog.pg_show_all_settings(
        OUT name text, OUT setting text, OUT unit text, OUT category text,
        OUT short_desc text, OUT extra_desc text, OUT context text,
        OUT vartype text, OUT source text, OUT min_val text, OUT max_val text,
        OUT enumvals text[], OUT boot_val text, OUT reset_val text,
        OUT sourcefile text, OUT sourceline integer)
RETURNS SETOF record AS
$BODY$
BEGIN
  RETURN QUERY
     SELECT *
       FROM pg_catalog.pg_show_all_settings() s
      WHERE s.context != 'postmaster'
        AND s.context != 'sighup';
END
$BODY$
LANGUAGE 'plpgsql' SECURITY DEFINER;

GRANT EXECUTE on FUNCTION storm_catalog.pg_show_all_settings() TO PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_show_all_settings() FROM PUBLIC;

CREATE VIEW storm_catalog.pg_settings AS
    SELECT *
      FROM pg_show_all_settings();

GRANT SELECT on storm_catalog.pg_settings TO PUBLIC;

REVOKE ALL on pg_catalog.pg_settings FROM public;

CREATE FUNCTION storm_catalog.pg_stat_get_activity(
        procpid integer, OUT datid oid, OUT pid integer, OUT usesysid oid,
        OUT application_name text, OUT state text, OUT query text,
		OUT waiting boolean, OUT xact_start timestamp with time zone,
        OUT query_start timestamp with time zone,
        OUT backend_start timestamp with time zone,
        OUT state_change timestamp with time zone,
		OUT client_addr inet,
        OUT client_hostname text, OUT client_port integer)
RETURNS SETOF record AS
$BODY$
BEGIN
  RETURN QUERY
     SELECT *
       FROM pg_catalog.pg_stat_get_activity($1) s
      WHERE s.datid = (SELECT oid
                       FROM pg_database
                      WHERE datname = current_database());
END
$BODY$
LANGUAGE 'plpgsql' SECURITY DEFINER;

GRANT EXECUTE on FUNCTION storm_catalog.pg_stat_get_activity(integer) TO PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_stat_get_activity(integer) FROM PUBLIC;

CREATE VIEW storm_catalog.pg_stat_activity AS
    SELECT *
      FROM storm_catalog.pg_stat_get_activity(NULL);

GRANT SELECT on storm_catalog.pg_stat_activity TO PUBLIC;

REVOKE ALL on pg_catalog.pg_stat_activity FROM public;

CREATE FUNCTION storm_catalog.pg_lock_status(
        OUT locktype text, OUT database oid, OUT relation oid,
        OUT page integer, OUT tuple smallint, OUT virtualxid text,
        OUT transactionid xid, OUT classid oid, OUT objid oid,
        OUT objsubid smallint, OUT virtualtransaction text,
        OUT pid integer, OUT mode text, OUT granted boolean,
		OUT fastpath boolean)
RETURNS SETOF record AS
$BODY$
BEGIN
  RETURN QUERY
     SELECT *
       FROM pg_catalog.pg_lock_status() l
      WHERE l.database = (SELECT oid
                            FROM pg_database
                           WHERE datname = current_database());
END
$BODY$
LANGUAGE 'plpgsql' SECURITY DEFINER;

GRANT EXECUTE on FUNCTION storm_catalog.pg_lock_status() TO PUBLIC;

REVOKE ALL on FUNCTION pg_catalog.pg_lock_status() FROM PUBLIC;

CREATE VIEW storm_catalog.pg_locks AS
    SELECT *
      FROM storm_catalog.pg_lock_status();

GRANT SELECT on storm_catalog.pg_locks TO PUBLIC;

REVOKE ALL on pg_catalog.pg_locks FROM public;
