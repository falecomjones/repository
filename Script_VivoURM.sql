WITH T1 AS (
SELECT 
    CONTACT.IDENTIFICATIONDOCUMENT1NUMBER AS CNPJ_CPF, --CUSTOMER     
    CONTACT.IDENTIFICATIONDOCUMENT2NUMBER AS DOC_ASSINANTE, --CUSTOMER
    CONTACT.IDENTIFICATION2TYPECODE AS TIPO_DOC_ASSINANTE, --CUSTOMER 
	CONTACT.PHONENUMBER AS CONTATO_TELEFONE, --CUSTOMER
	CONTACT.EMAILID AS CONTATO_EMAIL, --CUSTOMER
   	CUSTOMER.CUSTOMERLEGALNAME AS NOME_ASSINANTE, --CUSTOMER
    '' AS NOME_EMPRESA, --faltando --CUSTOMER
    CASE WHEN CUSTOMER.CUSTOMERTYPEKEY = 'C' THEN 2 
         WHEN CUSTOMER.CUSTOMERTYPEKEY = 'R' THEN 1 
         ELSE 9 END AS TIPO_ASSINANTE, --CUSTOMER
   SUB.LINEOFBUSINESSKEY AS TIPO_SERVICO, --MOBILE_LINE, FIXED_LINE
   SUB.SUBSCRIBERMSISDNALIAS AS NUM_SERVICO, --MOBILE_LINE, FIXED_LINE
-- SUB.OFFERTECHNOLOGY AS END_TIPO,   -- TRANSFORMAR, MAS QUAL A REGRA?  --MOBILE_LINE
   SUB.SUBSCRIBERKEY,
   SUB.MAINADDRESSKEY,   
   -- CASE WHEN STATUS.SUBSCRIBERSTATUSCHANGEDATE > ADDRHIST.STARTDATE THEN STATUS.SUBSCRIBERSTATUSCHANGEDATE ELSE ADDRHIST.STARTDATE END AS DT_INICIO, 
   STATUS.SUBSCRIBERSTATUSCHANGEDATE AS DT_INICIO, --MOBILE_LINE, FIXED_LINE
   LEAD (STATUS.SUBSCRIBERSTATUSCHANGEDATE, 1) OVER (PARTITION BY STATUS.SUBSCRIBERKEY ORDER BY STATUS.SUBSCRIBERSTATUSCHANGEDATE) AS DT_FIM, --MOBILE_MOVEMENT, FIXED_MOVEMENT
   -- CASE WHEN STATUS.LOADDATE < ADDRHIST.ENDDATE THEN STATUS.LOADDATE ELSE ADDRHIST.ENDDATE END AS DT_FIM,
   -- CASE WHEN SUB.OFFERTECHNOLOGY  = 'PostPaid' THEN 'POS'
   --     WHEN SUB.OFFERTECHNOLOGY = 'PrePaid' THEN 'PRE'
   --     ELSE 'XXX' END AS TIPO_PLANO, --MOBILE_LINE
   STATUS.CURRENTSUBSCRIBERSTATUSKEY AS SITUACAO --MOBILE_LINE, FIXED_LINE
   
FROM 
    CUSTOMER
    JOIN CONTACT 
	ON CONTACT.CONTACTKEY = CUSTOMER.MAINCONTACTKEY
    JOIN SUBSCRIBER SUB 
	ON SUB.CUSTOMERKEY = CUSTOMER.CUSTOMERKEY
    JOIN SUBSCRIBERSTATUSHIST STATUS
	ON STATUS.SUBSCRIBERKEY = SUB.SUBSCRIBERKEY    
WHERE    
    -- AND CONTACT.IDENTIFICATIONDOCUMENT1NUMBER = '87818023070'
    -- AND SUB.SUBSCRIBERMSISDNALIAS = '4135891122'
    --(ADRRHIST.SUBSCRIBERKEY IS NULL OR (ADDRHIST.STARTDATE <= STATUS.SUBSCRIBERSTATUSCHANGEDATE AND ADDRHIST.ENDDATE > STATUS.SUBSCRIBERSTATUSCHANGEDATE))
    -- AND STATUS.SUBSCRIBERSTATUSCHANGEDATE BETWEEN '1900-08-20 00:00:00.0' AND '2020-08-20 00:00:00.0'
	SUB.PRIMARYRESOURCEVALUE = '13997905003' AND SUB.SUBSCRIBERMSISDNALIAS = '13997905003'
), 
T2 AS 
(SELECT 
	T1.*, 
	ADDRHIST.SUBSCRIBERMAINADDRESSKEY,
	ADDRHIST.STARTDATE, --NETWORK_CIRCUIT 
	ADDRHIST.ENDDATE, --NETWORK_CIRCUIT
	IF(STARTDATE <= DT_FIM AND ENDDATE >= DT_INICIO, 'SIM', 'NAO') as MANTER
FROM 
	T1 
	LEFT JOIN SUBSCRIBERADDRESSHIST ADDRHIST 
	ON ADDRHIST.SUBSCRIBERKEY = T1.SUBSCRIBERKEY    
WHERE 
	--STARTDATE <= DT_FIM AND ENDDATE >= DT_INICIO AND
	SITUACAO = 'A' -- POSSIVELMENTE ADICIONAR OUTROS STATUS, COMO SUSPENSO)
),
T3 AS
(SELECT
	CNPJ_CPF, DOC_ASSINANTE, TIPO_DOC_ASSINANTE, NOME_ASSINANTE, NOME_EMPRESA, TIPO_ASSINANTE, TIPO_SERVICO, NUM_SERVICO, 
	IF(T2.DT_INICIO > STARTDATE, DT_INICIO, STARTDATE) AS DT_INICIO, 
	IF(T2.DT_FIM < ENDDATE, DT_FIM, ENDDATE) AS DT_FIM, 
	CONTATO_TELEFONE, CONTATO_EMAIL, SITUACAO, SUBSCRIBERMAINADDRESSKEY AS ADDRESSKEY
FROM 
	T2	
WHERE
	T2.MANTER = 'SIM'
UNION ALL
SELECT
	DISTINCT CNPJ_CPF, DOC_ASSINANTE, TIPO_DOC_ASSINANTE, NOME_ASSINANTE, NOME_EMPRESA, TIPO_ASSINANTE, TIPO_SERVICO,
			 NUM_SERVICO, DT_INICIO, DT_FIM, CONTATO_TELEFONE, CONTATO_EMAIL, SITUACAO, MAINADDRESSKEY AS ADDRESSKEY
FROM 
	T2	
WHERE
	T2.MANTER = 'NAO')

SELECT 
	CNPJ_CPF, DOC_ASSINANTE, TIPO_DOC_ASSINANTE, NOME_ASSINANTE, NOME_EMPRESA, TIPO_ASSINANTE, TIPO_SERVICO,
	NUM_SERVICO, DT_INICIO, DT_FIM, CONTATO_TELEFONE, CONTATO_EMAIL, SITUACAO, 
	--(não encontrados: TIPO DE ENDEREÇO, DESIGNADOR, ICCID, MACADDRESS, OPERADORA MVNE, ID SISTEMA ORIGEM, RPON, RPON Migrado (RNC))
	ADDR.ADDRESSLINE2 AS END_ENDERECO,
	ADDR.NEIGHBORHOODNAME AS END_BAIRRO,
	CITY.CITYDESC AS END_MUNICIPIO,
	STATE.STATECODE AS END_UF,
	ADDR.ZIPCODE AS END_CEP
FROM 
	T3
	JOIN ADDRESS ADDR ON T3.ADDRESSKEY = ADDR.ADDRESSKEY
	LEFT JOIN CITY ON ADDR.CITYKEY = CITY.CITYKEY
    LEFT JOIN STATE ON ADDR.STATEKEY = STATE.STATEKEY
; 
