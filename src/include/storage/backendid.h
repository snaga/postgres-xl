/*-------------------------------------------------------------------------
 *
 * backendid.h
 *	  POSTGRES backend id communication definitions
 *
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/backendid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKENDID_H
#define BACKENDID_H

/* ----------------
 *		-cim 8/17/90
 * ----------------
 */
typedef int BackendId;			/* unique currently active backend identifier */

#define InvalidBackendId		(-1)

extern PGDLLIMPORT BackendId MyBackendId;		/* backend id of this backend */

#ifdef XCP
/*
 * Two next variables make up distributed session id. Actual distributed
 * session id is a string, which includes coordinator node name, but
 * it is better to use Oid to store and compare with distributed session ids
 * of other backends under the same postmaster.
 */
extern PGDLLIMPORT Oid MyCoordId;

extern PGDLLIMPORT int MyCoordPid;

/* BackendId of the first backend of the distributed session on the node */
extern PGDLLIMPORT BackendId MyFirstBackendId;
#endif

#endif   /* BACKENDID_H */
