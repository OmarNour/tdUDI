insert into gdev1_etl.CDC_AUDIT
(
 SOURCE_NAME
,LOAD_ID
,BATCH_ID
,TABLE_NAME
,RECORDS_COUNT
,VALID_BATCH
,PROCESSED
)
select *
from
(
	select 
	case 
		when MIC_GANAME = 'AlternativeFamiliesDB' then 'AlternativeFamily'
		when MIC_GANAME = 'CSODB' then 'CSO'
		when MIC_GANAME = 'DXCDB_Inbound' then 'UHI_DXC'
		when MIC_GANAME = 'E-finance' then 'E-Finance_Mortabat'
		when MIC_GANAME = 'E-Finance' then 'NEW_MORTABAT'
		when MIC_GANAME = 'ElectricityDB' then 'Electricity_New'
		when MIC_GANAME = 'Fund_TreatmentDB' then 'FUNDED_TREATMENT'
		when MIC_GANAME = 'GAS_SubscriberDB' then 'Gas'
		when MIC_GANAME = 'GASVIPDB' then 'GasVIP'
		when MIC_GANAME = 'Health' then 'BIRTH_DEATH'
		when MIC_GANAME = 'Health' then 'HEALTH'
		when MIC_GANAME = 'HeyazaDB' then 'Heyaza_Agriculture'
		when MIC_GANAME = 'InsurancePensionGDB' then 'Insurance_Pension'
		when MIC_GANAME = 'MOSSDB' then 'E-FINANCE_TK_TRANSACTION'
		when MIC_GANAME = 'MOSSDB' then 'TAKAFUL_KARAMA'
		when MIC_GANAME = 'RealStateinvstDB' then 'Tamweel'
		when MIC_GANAME = 'SupplyDB' then 'Tamwin'
		when MIC_GANAME = 'Telecom_Consolidated' then 'Telecom'
		when MIC_GANAME = 'TrafficDB' then 'MROR'
		when MIC_GANAME = 'VACCINE' then 'VACCINE'
		when MIC_GANAME = 'WaitingListDB' then 'Waiting_Lists'
	end SOURCE_NAME
	, MIC_EXECUTIONGUID LOAD_ID
	, MIC_RUNID BATCH_ID
	, TERA_TABLE_NAME TABLE_NAME
	, TERA_INPUT_RECORD_COUNT RECORDS_COUNT
	, case when TERA_JOB_END_TIME is not null and DB2_TO_TERA_STATUS like '%success%' then 1 else 0 end VALID_BATCH
	, 0 PROCESSED
	from dev_stg_online.ibm_audit_table
	where coalesce(SOURCE_NAME,'') <> ''
	and coalesce(TABLE_NAME,'') <> ''
	and coalesce(LOAD_ID,'') <> ''
) src

where not exists (
								select 1 
								from gdev1_etl.CDC_AUDIT tgt 
								where tgt.SOURCE_NAME=src.SOURCE_NAME
								and tgt.LOAD_ID=src.LOAD_ID								
								);