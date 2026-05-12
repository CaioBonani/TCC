-- public.aih definição

-- Drop table

-- DROP TABLE public.aih;

CREATE TABLE public.aih (
	ano_cmpt int4 NULL,
	mes_cmpt int4 NULL,
	dt_inter varchar(8) NULL,
	dt_saida varchar(8) NULL,
	cep varchar(8) NULL,
	munic_res varchar(6) NULL,
	munic_mov varchar(6) NULL,
	cgc_hosp varchar(14) NULL,
	cnes varchar(7) NULL,
	nasc varchar(8) NULL,
	sexo int2 NULL,
	idade int2 NULL,
	cod_idade int2 NULL,
	nacional varchar(3) NULL,
	instru int2 NULL,
	raca_cor int2 NULL,
	etnia varchar(4) NULL,
	cbor varchar(6) NULL,
	morte int2 NULL,
	uti_mes_to int2 NULL,
	marca_uti int2 NULL,
	val_uti numeric(12, 2) NULL,
	proc_solic varchar(10) NULL,
	proc_rea varchar(10) NULL,
	val_sh numeric(12, 2) NULL,
	val_sp numeric(12, 2) NULL,
	n_aih varchar(13) NULL,
	val_tot numeric(12, 2) NULL,
	infehosp int2 NULL,
	ind_vdrl int2 NULL,
	diag_princ varchar(4) NULL,
	diag_secun varchar(4) NULL,
	diagsec1 varchar(4) NULL,
	diagsec2 varchar(4) NULL,
	diagsec3 varchar(4) NULL,
	diagsec4 varchar(4) NULL,
	diagsec5 varchar(4) NULL,
	diagsec6 varchar(4) NULL,
	diagsec7 varchar(4) NULL,
	diagsec8 varchar(4) NULL,
	diagsec9 varchar(4) NULL,
	cid_morte varchar(4) NULL
)
PARTITION BY LIST (ano_cmpt);