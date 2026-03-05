`pg-proxy` is a postgres protocol Proxy server, able to translate on the fly, some incompatible queries coming from a legacy application used with another database engine

## Usage

```
Usage of ./pg-proxy:
  -certificate string
        SSL certificate file *.crt (default none)
  -host string
        Listener host (default all local interfaces)
  -keeporiginal
        Keep the original query at the end in a multiline SQL comment (default true)
  -key string
        SSL key file *.key (default none)
  -override string
        Startup parameters override, in JSON format (default "{}")
  -polyfill
        Polyfills already applied by a system account (default true)
  -port int
        Listener port (default 5432)
  -remote string
        Proxy remote address (default none)
  -translator string
        Query translator ('ingres' or 'iso') (default "iso")
  -verbose int
        Verbosity: 0 = none, 1 = connections, 2 = translated queries, 4 = all queries (default 0)
  -web-port int
        Web UI port for query monitoring (0 = disabled)
  -web-secret string
        Web UI secret (default none)
```

## Ingres Translator

### How to launch

Lancement en mode Ingres: `pg-proxy -translator ingres -override '{"datestyle":"iso,us"}' -remote postgreshost:5432` (on surcharge datestyle pour simuler le date_format=US par défaut dans la base Ingres)

sur AIX: `nohup ./pg-proxy -translator ingres -override '{"datestyle":"iso,us"}' -remote mnusvlingest03:5432 -web-port 5000 -web-secret ingres -verbose 3 &`

### Translated syntax

* Parsing multi ordres: `SAVEPOINT XXX; CREATE TABLE YYY; SET ZZZ=`, etc.
* `DECLARE GLOBAL` → `CREATE`
* Options `WITH ...` supprimés, sauf `NOJOURNALING`, conservé via un `CREATE UNLOGGED TABLE`
* Inversion `CREATE... AS ... ON ...` → `CREATE... ON ... AS ...` (le `ON` doit etre avant le `AS` sous Postgres)
* Inversion `UPDATE FROM SET` → `UPDATE SET FROM` (+ suppression des alias si il y en a, sur la clause SET: `update from table a set a.col = 1` → `update set col = 1 from table a`)
* `COPY...INTO` → `COPY...TO`
* `SET LOCKMODE` bypassé (on remplace par `SET pg.xxx = yyy` si on arrive à en extraire les valeurs, sinon `pg.dummy=1`)
* Suppression des alias commencant par `session.`
* `MODIFY` → `CREATE INDEX`
* `EXECUTE PROCEDURE` → `CALL` et syntaxe des arguments: `=` → `=>`
* `ingresdate` → `date`
* `sysdate` → `current_timestamp`
* Refactoring/support des fonctions `charextract`, `char`, `vchar`, `varchar`, `date_part`, `squeeze`, `date_format`, `ifnull`, `lowercase`, `uppercase`, `decimal`, `timestampadd`. NB:  `char`, `vchar`, `varchar` ne seront pas touchés si ils sont dans un bloc DDL (`CREATE TABLE <...>`)
* Refactoring/support des fonctions `right`/`left` en mode conservation de largeur du char (via l'intermédiaire d'un `format('%s', xxx)`)
* Gestion de l'appel à une séquence via `maseq.nextval` → `nextval('maseq')`
* Support des tables systèmes `iitables` et `iicolumns`
* Support fonction `dbmsinfo` avec les paramètres `SESSION_ID` et `_version`
* `COPY` compatible

## Non supporté

* CHAR/VARCHAR trop longs automatiquement tronqués

## TOCHECK

https://github.com/ActianCorp/sqlalchemy-ingres/blob/master/lib/sqlalchemy_ingres/base.py

### iitables

```sql
table_name	table_name	0	char				256	256	0	CHAR			
table_owner	table_owner	1	char				32	32	0	CHAR			
create_date	create_date	2	char				25	25	0	CHAR			
alter_date	alter_date	3	char				25	25	0	CHAR			
table_type	table_type	4	char				1	1	0	CHAR			
table_subtype	table_subtype	5	char				1	1	0	CHAR			
table_version	table_version	6	varchar				11	11	0	VARCHAR			
system_use	system_use	7	char				1	1	0	CHAR			
table_stats	table_stats	8	char				1	1	0	CHAR			
table_indexes	table_indexes	9	char				1	1	0	CHAR			
is_readonly	is_readonly	10	char				1	1	0	CHAR			
concurrent_access	concurrent_access	11	char				1	1	0	CHAR			
num_rows	num_rows	12	bigint				20	19	0	BIGINT			
storage_structure	storage_structure	13	char				16	16	0	CHAR			
is_compressed	is_compressed	14	char				1	1	0	CHAR			
key_is_compressed	key_is_compressed	15	char				1	1	0	CHAR			
duplicate_rows	duplicate_rows	16	char				1	1	0	CHAR			
unique_rule	unique_rule	17	char				1	1	0	CHAR			
number_pages	number_pages	18	integer				11	10	0	INTEGER			
overflow_pages	overflow_pages	19	integer				11	10	0	INTEGER			
row_width	row_width	20	integer				11	10	0	INTEGER			
expire_date	expire_date	21	bigint				20	19	0	BIGINT			
modify_date	modify_date	22	char				25	25	0	CHAR			
location_name	location_name	23	varchar				32	32	0	VARCHAR			
table_integrities	table_integrities	24	char				1	1	0	CHAR			
table_permits	table_permits	25	char				1	1	0	CHAR			
all_to_all	all_to_all	26	char				1	1	0	CHAR			
ret_to_all	ret_to_all	27	char				1	1	0	CHAR			
is_journalled	is_journalled	28	char				1	1	0	CHAR			
view_base	view_base	29	char				1	1	0	CHAR			
multi_locations	multi_locations	30	char				1	1	0	CHAR			
table_ifillpct	table_ifillpct	31	smallint				6	5	0	SMALLINT			
table_dfillpct	table_dfillpct	32	smallint				6	5	0	SMALLINT			
table_lfillpct	table_lfillpct	33	smallint				6	5	0	SMALLINT			
table_minpages	table_minpages	34	integer				11	10	0	INTEGER			
table_maxpages	table_maxpages	35	integer				11	10	0	INTEGER			
table_relstamp1	table_relstamp1	36	bigint				20	19	0	BIGINT			
table_relstamp2	table_relstamp2	37	bigint				20	19	0	BIGINT			
table_reltid	table_reltid	38	integer				11	10	0	INTEGER			
table_reltidx	table_reltidx	39	integer				11	10	0	INTEGER			
unique_scope	unique_scope	40	char				1	1	0	CHAR			
allocation_size	allocation_size	41	integer				11	10	0	INTEGER			
extend_size	extend_size	42	integer				11	10	0	INTEGER			
allocated_pages	allocated_pages	43	integer				11	10	0	INTEGER			
label_granularity	label_granularity	44	char				1	1	0	CHAR			
row_security_audit	row_security_audit	45	char				1	1	0	CHAR			
security_label	security_label	46	char				8	8	0	CHAR			
table_pagesize	table_pagesize	47	integer				11	10	0	INTEGER			
table_relversion	table_relversion	48	smallint				6	5	0	SMALLINT			
table_reltotwid	table_reltotwid	49	integer				11	10	0	INTEGER			
table_reltcpri	table_reltcpri	50	smallint				6	5	0	SMALLINT			
tups_per_page	tups_per_page	51	integer				11	10	0	INTEGER			
keys_per_page	keys_per_page	52	integer				11	10	0	INTEGER			
keys_per_leaf	keys_per_leaf	53	integer				11	10	0	INTEGER			
phys_partitions	phys_partitions	54	smallint				6	5	0	SMALLINT			
partition_dimensions	partition_dimensions	55	smallint				6	5	0	SMALLINT			
table_reldatawid	table_reldatawid	56	integer				11	10	0	INTEGER			
table_reltotdatawid	table_reltotdatawid	57	integer				11	10	0	INTEGER			
encrypted_columns	encrypted_columns	58	char				1	1	0	CHAR			
encryption_version	encryption_version	59	smallint				6	5	0	SMALLINT			
encryption_type	encryption_type	60	varchar				6	6	0	VARCHAR			
minmax_samples	minmax_samples	61	char				1	1	0	CHAR			
```

### iicolumns

```sql
table_name	table_name	0	char				256	256	0	CHAR			
table_owner	table_owner	1	char				32	32	0	CHAR			
column_name	column_name	2	char				256	256	0	CHAR			
column_datatype	column_datatype	3	char				32	32	0	CHAR			
column_length	column_length	4	integer				11	10	0	INTEGER			
column_scale	column_scale	5	integer				11	10	0	INTEGER			
column_collid	column_collid	6	smallint				6	5	0	SMALLINT			
column_nulls	column_nulls	7	char				1	1	0	CHAR			
column_defaults	column_defaults	8	char				1	1	0	CHAR			
column_sequence	column_sequence	9	integer				11	10	0	INTEGER			
key_sequence	key_sequence	10	integer				11	10	0	INTEGER			
sort_direction	sort_direction	11	char				1	1	0	CHAR			
column_ingdatatype	column_ingdatatype	12	integer				11	10	0	INTEGER			
column_internal_datatype	column_internal_datatype	13	char				32	32	0	CHAR			
column_internal_length	column_internal_length	14	integer				11	10	0	INTEGER			
column_internal_ingtype	column_internal_ingtype	15	smallint				6	5	0	SMALLINT			
column_system_maintained	column_system_maintained	16	char				1	1	0	CHAR			
column_updateable	column_updateable	17	char				1	1	0	CHAR			
column_has_default	column_has_default	18	char				1	1	0	CHAR			
column_default_val	column_default_val	19	varchar				1,501	1,501	0	VARCHAR			
security_audit_key	security_audit_key	20	char				1	1	0	CHAR			
column_always_ident	column_always_ident	21	char				1	1	0	CHAR			
column_bydefault_ident	column_bydefault_ident	22	char				1	1	0	CHAR			
column_encrypted	column_encrypted	23	char				1	1	0	CHAR			
column_encrypt_width	column_encrypt_width	24	integer				11	10	0	INTEGER			
column_encrypt_salt	column_encrypt_salt	25	char				1	1	0	CHAR			
column_encrypt_crc	column_encrypt_crc	26	char				1	1	0	CHAR			
column_has_mask	column_has_mask	27	char				1	1	0	CHAR			
column_mask_val	column_mask_val	28	varchar				1,501	1,501	0	VARCHAR			
column_has_minmax	column_has_minmax	29	char				1	1	0	CHAR			
```