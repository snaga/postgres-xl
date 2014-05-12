/* contrib/pghba/pghba--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pghba" to load this file. \quit

ALTER EXTENSION pghba ADD view pghba;
