/* contrib/pghba/pghba--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pghba" to load this file. \quit

-- Register the function.
CREATE FUNCTION pghba_list()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pghba_list'
LANGUAGE C;

-- Create a view for convenient access.
CREATE VIEW pghba AS
        SELECT H.* FROM pghba_list() AS H
        (linenumber integer, conntype integer, databases text, roles text,
	 addr text, mask text, ip_cmp_method integer, hostname text,
	 auth_method integer, usermap text, pamservice text, ldaptls boolean,
	 ldapserver text, ldapport integer, ldapbinddn text, ldapbindpasswd text,
	 ldapsearchattribute text, ldapbasedn text, ldapprefix text,
	 ldapsuffix text, clientcert boolean, krb_server_hostname text,
	 krb_realm text, include_realm boolean, radiusserver text,
	 radiussecret text, radiusidentifier text, radiusport integer);

-- Don't want these to be available to public.
REVOKE ALL on FUNCTION pghba_list() FROM public;
REVOKE ALL on pghba FROM public;
