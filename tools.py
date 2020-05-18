from pyspark import SparkContext
from pyspark.sql import SparkSession

class tools:
    def getSpark(self):
        SparkContext.setSystemProperty("hive.metastore.uris", "thrift://localhost:9083")

        spark = SparkSession \
                        .builder \
                        .appName('example-pyspark-read-and-write-from-hive') \
                        .config("spark.sql.warehouse.dir", "spark_warehouse") \
                        .enableHiveSupport() \
                        .getOrCreate()
        #spark.conf.get("spark.sql.hive.metastore.version")
        spark.sql("SET spark.sql.hive.metastore.version=2.3.2").show()

        #spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
        return spark

    def getTable(self):
        return 0
    def test(self):
        spark = tools.getSpark(None)
        df_customer = spark.sql('SELECT * FROM customer')
        df_contact = spark.sql('select * from contact')
        df_subscriber = spark.sql('select * from subscriber')
        df_subscriberstatushist = spark.sql('select * from subscriberstatushist')

        #df_customer.show()
        df_custo_contac = df_customer.join(df_contact, df_customer.maincontactkey == df_contact.contactkey)
        df_custo_contac_subsc = df_custo_contac.join(df_subscriber, ['customerkey'])
        df_custo_contac_subsc_subshist = df_custo_contac_subsc.join(df_subscriberstatushist, ['subscriberkey'])
        #df_custo_contac_subsc_subshist.show()
        #cria uma tabela temporaria e uma tabela fisica no hive
        #df_custo_contac_subsc_subshist.createOrReplaceTempView('temptable')
        #spark.sql('create table final_table as select * from temptable')

        #t2
        df_subsc_addr = spark.sql('select *, IF(STARTDATE <= DT_FIM AND ENDDATE >= DT_INICIO, "SIM", "NAO") as MANTER from subscriberaddresshist')
        df_subsc_addr.show()
        df_t2 = df_custo_contac_subsc_subshist.join(df_subsc_addr, ['SUBSCRIBERKEY'], how='left')
        df_t2.show()

        #df_subscriber.show()
        #df_subscriberstatushist.show()

        #CUSTOMER
        #JOIN CONTACT ON CONTACT.CONTACTKEY = CUSTOMER.MAINCONTACTKEY

        #logfile = environ['SPARK_HOME'] + "/README.md"
        #df = spark.read.csv(logfile)
        #df_load.show()



