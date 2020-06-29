from os.path import abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame


# ----------------------------------------------------------------------------
# Define Spark Session
# ----------------------------------------------------------------------------
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Level2 Graph Constructor") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.addPyFile("/HDFS_ROOT/EA/supplychain/zeppelin/pyfiles/"
                             + "graphframes.zip")


from graphframes import *


def LUT(df, column):
    """
    FUNCTION: LUT

    DESCRIPTION:
        Assign unique ID to text, and create a "Look Up Table" to the text.
        """
    id = column + "_id"
    df = df.select(F.col(column))
    LUT = sqlContext.createDataFrame(df.rdd.map(lambda x: x[0]).zipWithIndex(),
                                     StructType([StructField(column,
                                                             StringType(),
                                                             True),
                                                 StructField(id,
                                                             IntegerType(),
                                                             True)]))
    return LUT


def createVertices(df, id, name, attributes, *parentid):
    """
    FUNCTION: createVertices

    DESCRIPTION:
        Vertice constructor from pyspark dataframe.

    ARGUMENTS:
        # ---------------------------------------------------------------------
        # ARGUMENTS      | TYPES             | DESCRIPTION
        # ---------------------------------------------------------------------
        # df             | pyspark dataframe | DF to build node from.
        # id             | string            | DF column Name of "key" of node.
        # name           | string            | label of node.
        # attributes     | dict              | dict of cols & Types to struct
        # parentid       | * string          | the parent node id.
        # ---------------------------------------------------------------------

        * optional arg

    RETURNS:
        pyspark dataframe "v" ("id", "name", "attributes")

    EXAMPLES:
        # ---------------------------------------------------------------------
        # create vertices and printSchema
        # ---------------------------------------------------------------------
        # dict of req edge col(s)
        <<<< attr = {"flowtype": StringType(), "plant": StringType()}
        <<<< vertices = createVertices(l1Bridgenew,
                                       'Output.Header.Keys.hlikey',
                                       'HLIFulfillment',
                                       attr).printSchema()
        root
         |-- id: string (nullable = true)
         |-- name: string (nullable = false)
         |-- attributes: struct (nullable = false)
         |    |-- flowtype: string (nullable = true)
         |    |-- plant: string (nullable = true)
    """
    # -------------------------------------------------------------------------
    # Cleanup id string
    # -------------------------------------------------------------------------
    v = df.withColumn("cleanID",
                      F.regexp_replace
                      (F.trim
                       (F.regexp_replace
                        (F.col(id), '\([^)]*\)', '')), ' ', '_'))
    # -------------------------------------------------------------------------
    # Create Vertices DF with columns id & name
    # -------------------------------------------------------------------------
    v = v.withColumn("id", F.col("cleanID"))
    v = v.withColumn("name", F.lit(name))
    # set parentid to "" if not passed
    if len(parentid) == 0:
        parentid = F.lit("")
    else:
        parentid = F.col(parentid[0])
    v = v.withColumn("parentid", parentid)
    # -------------------------------------------------------------------------
    # change struct to map types
    # -------------------------------------------------------------------------
    v = v.rdd.map(lambda row: row.asDict(recursive=True)
                  ).toDF(sampleRatio=0.8)
    # -------------------------------------------------------------------------
    # Create col "attributes" Struct from arg attributes.
    # If df.column exist, use column value, else add None with provided Type()
    # -------------------------------------------------------------------------
    for key in attributes:
        if key not in v.columns:
            v = v.withColumn(key,
                             F.lit(None).cast(attributes.get(key)).alias(key))
    v = v.withColumn("attributes",
                     F.struct
                     ([F.col(k) for k in sorted(attributes.keys())]
                      ))
    # -------------------------------------------------------------------------
    # Select "id", "name", "attributes"
    # -------------------------------------------------------------------------
    v = v.select("id", "name", "parentid", "attributes")
    return v


def createEdges(srcdf, srcid, srckey, dstdf, dstid, dstkey, relationship):
    """
    FUNCTION: createEdges

    DESCRIPTION:
        Edge constructor from pyspark dataframe.

    ARGUMENTS:
        # ---------------------------------------------------------------------
        # ARGUMENTS      | TYPES              | DESCRIPTION
        # ---------------------------------------------------------------------
        # srcdf          | pyspark dataframe  | DF of src node.
        # srcid          | string             | DF column from src "id"
        # srckey         | string             | DF column key to connect to dst
        # dstdf          | pyspark dataframe  | DF of dst node.
        # dstid          | string             | DF column from dst "id"
        # dstkey         | string             | DF column key to connect to src
        # relationship   | string             | label of edge connection.
        # ---------------------------------------------------------------------

    RETURNS:
        pyspark dataframe "e" ("src", "dst", "relationship")

    EXAMPLES:
        # ---------------------------------------------------------------------
        # create edge and printSchema
        # ---------------------------------------------------------------------
        <<<< edges = createEdges(l1.where("flowtype" == "BridgeDirect"),
                                 "Output.Header.Keys.hlikey",
                                 "Output.Header.Keys.bridgekey",
                                 l1.where("flowtype" == "BridgeFulfillDirect"),
                                 "Output.Header.Keys.hlikey",
                                 "Output.Header.Keys.bridgekey",
                                 "BridgeOrder").printSchema()
        root
         |-- src: string (nullable = true)
         |-- dst: string (nullable = true)
         |-- relationship: string (nullable = false)
    """
    # -------------------------------------------------------------------------
    # Create Edge DF with columns relationship from "srcdf"
    # -------------------------------------------------------------------------
    e = srcdf.select(F.col(srcid).alias("src"),
                     F.col(srckey).alias("srckey")).distinct()
    e = e.withColumn("relationship", F.lit(relationship))
    # -------------------------------------------------------------------------
    # dstdf Prepare
    # -------------------------------------------------------------------------
    dstdf = dstdf.select(F.col(dstid).alias("dst"),
                         F.col(dstkey).alias("dstkey")).distinct()
    if dstdf.select("dstkey").dtypes[0][1] == 'array<string>':
        joinsOn = F.expr("array_contains(dstkey, srckey)")
    else:
        joinsOn = e.srckey == dstdf.dstkey
    # -------------------------------------------------------------------------
    # left join Edge DF with dstdf
    # -------------------------------------------------------------------------
    e = e.join(dstdf, joinsOn,
               how="left").select(F.col("src"),
                                  F.col("dst"),
                                  F.col("relationship"))
    return e


def parseJsonCols(df, *cols, **sanitize):
    """
    FUNCTION: parseJsonCols

    ARGUMENTS:
        # ---------------------------------------------------------------------
        # ARGUMENTS      | TYPES             | DESCRIPTION
        # ---------------------------------------------------------------------
        # df             | pyspark dataframe | DF containing the JSON cols.
        # ---------------------------------------------------------------------
        # *cols          | string(s)         | DF column Names containing JSON.
        # ---------------------------------------------------------------------
        # **sanitize     | boolean           | Flag indicating to sanitize JSON
        #----------------------------------------------------------------------

    DESCRIPTION:
        Auto infer the schema of a json column and parse into a struct.

    RETURNS:
        pyspark dataframe with the decoded columns.

    EXAMPLES:
        # ---------------------------------------------------------------------
        # before df.printSchema()
        # ---------------------------------------------------------------------
        >>>> l1HLIDeflate.printSchema()
        root
         |-- value: string (nullable = true)
        # ---------------------------------------------------------------------
        # after df.printSchema()
        # ---------------------------------------------------------------------
        >>>> l1HLI = parseJsonCols(l1HLIDeflate,"value",sanitize=True)
        >>>> l1HLI.printSchema()
        root
         |-- InfoStates: struct (nullable = true)
         |    |-- bloodbuild: array (nullable = true)
         |    |    |-- element: struct (containsNull = true)
         |    |    |    |-- endtime: string (nullable = true)
         |    |    |    |-- starttime: string (nullable = true)
         |    |    |    |-- statevalue: string (nullable = true)
         |    |    |    |-- trace: string (nullable = true)
         |    |-- emr: array (nullable = true)
         |    |    |-- element: struct (containsNull = true)
         |    |    |    |-- endtime: string (nullable = true)
         |    |    |    |-- starttime: string (nullable = true)
         |    |    |    |-- statevalue: string (nullable = true)
         |    |    |    |-- trace: string (nullable = true)
    """
    # -------------------------------------------------------------------------
    # parseJSONCols
    # -------------------------------------------------------------------------
    res = df
    for i in cols:
        # sanitize if requested.
        if sanitize:
            res = (
                res.withColumn(
                    i,
                    F.concat(F.lit('{"data": '), i, F.lit('}'))
                )
            )
        # ---------------------------------------------------------------------
        # infer schema and apply it
        # ---------------------------------------------------------------------
        schema = spark.read.json(res.rdd.map(lambda x: x[i])).schema
        res = res.withColumn(i, F.from_json(F.col(i), schema))
        # ---------------------------------------------------------------------
        # unpack the wrapped object if needed
        # ---------------------------------------------------------------------
        if sanitize:
            res = res.withColumn(i, F.col(i).data)
    return res


def combineMap(col, typeobj):
    """
    FUNCTION: combineMap

    DESCRIPTION:
        creates pyspark udf that collects maptypes to combine in a single col.

    EXAMPLES:
        # ---------------------------------------------------------------------
        # Example types from dict
        # ---------------------------------------------------------------------
        >>>> milestoneMap = combineMap("maps", attributes.get("Milestones"))
        # ---------------------------------------------------------------------
        # Example passing types directly
        # ---------------------------------------------------------------------
        >>>> msChildMap = combineMap("maps", MapType(StringType(),
                                                     StringType(), True))
        # ---------------------------------------------------------------------
        # Example using UDF in DF
        # ---------------------------------------------------------------------
        >>>> ms.groupBy("id").agg(F.collect_list('MS').alias('maps')) \
                .select('id', F.create_map(F.col("milestone"),
                        msChildMap('maps')).alias('combined_map')).show(3, 50)
        +----------------+--------------------------------------------------+
        |              id|                                      combined_map|
        +----------------+--------------------------------------------------+
        |7200000513000240|[itemcleaned -> [endtime -> 20191023151734, sta...|
        +----------------+--------------------------------------------------+
    """
    # -------------------------------------------------------------------------
    # combineMap pyspark udf
    # -------------------------------------------------------------------------
    fname = F.udf(lambda col: {key: f[key] for f in col for key in f},
                  typeobj)
    return fname


def combineLists(*lists):
    """
    FUNCTION: combineLists

    DESCRIPTION:
        returns Set from appened list items
        """
    mergedlist = []
    for list in lists:
        for item in list:
            mergedlist.append(item)
    mergedSet = set(mergedlist)
    return mergedSet


def rename(df, columnNames):
    """
    FUNCTION: rename

    DESCRIPTION:
        rename columns with "oldColumnNames","newColumnNames"
        """
    # -------------------------------------------------------------------------
    # for newColumnName in oldColumnNames zip
    # -------------------------------------------------------------------------
    for c, n in columnNames:
        df = df.withColumnRenamed(c, n)
    return df


def mirrorSchema(df1, df2):
    """
    FUNCTION: mirrorSchema

    DESCRIPTION:
        """
    maxSchema = df2.schema
    df1new = spark.createDataFrame(df1.rdd, maxSchema)
    return df1new


def unionAll(*dfs):
    """
    FUNCTION: unionAll

    DESCRIPTION:
        """
    return reduce(DataFrame.unionAll, dfs)


def dfColumnToList(dataFrame, headerColumn):
    """
    FUNCTION: dfColumnToList

    DESCRIPTION:
        """
    dfCollect = dataFrame.collect()
    header = [Dict[headerColumn] for Dict in dfCollect]
    return header


def createMotif(length, **degree):
    """
    FUNCTION: createMotif

    ARGUMENTS:
        # ---------------------------------------------------------------------
        # ARGUMENTS      | TYPES             | DESCRIPTION
        # ---------------------------------------------------------------------
        # length         | long()            | the range of the search motifs
        # ---------------------------------------------------------------------

    DESCRIPTION:
        auto create motif search pattern based off length.

    RETURNS:
        motif path based of provied length

    EXAMPLES:
        "(start)-[]->(n2); (n2)-[]->(n3); (n3)-[]->(end)"
    """
    # -------------------------------------------------------------------------
    # set degree
    # -------------------------------------------------------------------------
    if degree.get("key", "") == "in":
        degree = "in"
    else:
        degree = "out"
    # -------------------------------------------------------------------------
    if degree == "out":
        if length == 0:
            motifPath = "(start)"
        else:
            motifPath = "(start)-[]->"
            for i in range(1, length):
                motifPath += "(n%s);(n%s)-[]->" % (i - 1, i - 1)
            motifPath += "(end)"
    else:
        motifPath = "(end)-[]->(start)"
    return motifPath


def findMembers(graph, length, startname, **attrDict):
    """
    FUNCTION: findMembers

    ARGUMENTS:
        # ---------------------------------------------------------------------
        # ARGUMENTS      | TYPES             | DESCRIPTION
        # ---------------------------------------------------------------------
        # graph          | graphFrame graph  | graph
        # ---------------------------------------------------------------------
        # length         | long()            | the range of the search motifs
        # ---------------------------------------------------------------------
        # startname      | string()          | filter node to start search from
        #----------------------------------------------------------------------

    DESCRIPTION:

    RETURNS:
        pyspark dataframe with groupID, memberID with found connected
        Members.

        root
         |-- groupID: string (nullable = true)
         |-- groupactivecode: string (nullable = false)
         |-- groupactivetimestamp: string (nullable = false)
         |-- memberID: struct (nullable = false)
         |    |-- HLITrade: string (nullable = true)
         |    |-- HLIFulfillment: string (nullable = true)
         |    |-- FactoryDelivery: string (nullable = true)

    EXAMPLES:
         +-------+-------------------------+------------------------------+
         |groupID                          | memberID                     |
         +-------+-------------------------+------------------------------+
         |300-7100002695000050-BridgeDirect|[7100002695000050, 4066166...]|
         |300-7200000388000080-BridgeDirect|[7200000388000080, 4067035...]|
         |300-7200000376000100-BridgeDirect|[7200000376000100, 4067048...]|
         +---------------------------------+------------------------------+
          only showing top 3 rows
    """
    # -------------------------------------------------------------------------
    # setup & default variables
    # -------------------------------------------------------------------------
    inleng = 0
    columns = ""
    out_motifs = {}
    in_motifs = {}
    vCollect = graph \
        .vertices \
        .withColumn("groupID",
                    F.when(F.col("name") == startname,
                           F.concat_ws("-",
                                       "attributes.client",
                                       "id",
                                       "attributes.flowtype"))
                    ).select("id", "name", "groupID", "attributes")
    eCollect = graph.edges
    # -------------------------------------------------------------------------
    # gCollect graph
    # -------------------------------------------------------------------------
    gCollect = GraphFrame(vCollect, eCollect)
    # -------------------------------------------------------------------------
    # dynamic edges out search motif
    # -------------------------------------------------------------------------
    for leng in reversed(range(length)):
        motifPath = createMotif(leng)
        membersMotifs = gCollect.find(motifPath
                                      ).where(F.col("start.name"
                                                    ) == startname)
        out_motifs[leng] = membersMotifs
    # -------------------------------------------------------------------------
    # list of names & ids from motif output
    # -------------------------------------------------------------------------
    for k in out_motifs:
        clist = list(enumerate(out_motifs[k].columns, 1))
        for v in clist:
            name = v[1] + ".name"
            id = v[1] + ".id"
            attr = v[1] + ".attributes"
            results = {}
            namevalue = out_motifs[k].select(F.col(name)).distinct().collect()
            for i in namevalue:
                results.update(i.asDict())
            if len(results) > 0:
                memberID = results['name']
                # -------------------------------------------------------------
                # dynamic edges in per memberID search motif
                # -------------------------------------------------------------
                inleng += length
                motifInPath = createMotif(1, key="in")
                membersInMotifs = gCollect.find(motifInPath
                                                ).where(F.col("start.name"
                                                              ) == memberID)
                in_motifs[inleng] = membersInMotifs
                # -------------------------------------------------------------
                inlist = list(enumerate(in_motifs[inleng].columns, 1))
                for v in inlist:
                    inName = v[1] + ".name"
                    inID = v[1] + ".id"
                    inAttr = v[1] + ".attributes"
                    inresults = {}
                    innamevalue = in_motifs[inleng] \
                        .select(F.col(inName)).distinct().collect()
                    for i in innamevalue:
                        inresults.update(i.asDict())
                    if len(inresults) > 0:
                        inmemberID = inresults['name']
                    # ---------------------------------------------------------
                    # in memberIDs
                    # ---------------------------------------------------------
                    if "memberID" in in_motifs[inleng].columns:
                        s_fields = in_motifs[inleng] \
                            .schema["memberID"].dataType.names
                        in_motifs[inleng] = in_motifs[inleng] \
                            .withColumn("memberID",
                                        F.struct(*([F.col('memberID'
                                                          )[c].alias(c)
                                                    for c in s_fields]
                                                   + [F.col(inID
                                                            ).alias(inmemberID
                                                                    )])))
                    else:
                        in_motifs[inleng] = in_motifs[inleng] \
                            .withColumn("memberID",
                                        F.struct(F.when
                                                 (F.col(inID).isNotNull(),
                                                  F.col
                                                  (inID).alias
                                                  (inmemberID)
                                                  ).otherwise(F.lit(None))
                                                 .alias(inmemberID)))
                    # ---------------------------------------------------------
                    # in accumulatedProperties
                    # ---------------------------------------------------------
                    columns = [F.col(inAttr + "." + c
                                     ) for c in attrDict[inmemberID]]
                    if "accumulatedProperties" in in_motifs[inleng].columns:
                        s_fields = in_motifs[inleng]\
                            .schema["accumulatedProperties"] \
                            .dataType.names
                        in_motifs[inleng] = in_motifs[inleng] \
                            .withColumn("accumulatedProperties",
                                        F.struct(*([F.col
                                                    ("accumulatedProperties"
                                                     )[c].alias(c)
                                                    for c in s_fields]
                                                   + [F.when
                                                      (F.struct(columns)
                                                       .isNotNull(),
                                                       F.struct(columns))
                                                       .otherwise
                                                       (F.lit(None))
                                                       .alias(inmemberID)]
                                                   )))
                else:
                    in_motifs[inleng] = in_motifs[inleng] \
                        .withColumn("accumulatedProperties",
                                    F.struct(F.when
                                             (F.struct(columns)
                                              .isNotNull(),
                                              F.struct(columns)
                                              ).otherwise(F.lit(None))
                                             .alias(inmemberID)))
                # -------------------------------------------------------------
                # out memberIDs
                # -------------------------------------------------------------
                if "memberID" in out_motifs[k].columns:
                    s_fields = out_motifs[k].schema["memberID"].dataType.names
                    out_motifs[k] = out_motifs[k] \
                        .withColumn("memberID",
                                    F.struct(*([F.col('memberID')[c].alias(c)
                                                for c in s_fields]
                                               + [F.col(id).alias(memberID)])))
                else:
                    out_motifs[k] = out_motifs[k] \
                        .withColumn("memberID",
                                    F.struct(F.when
                                             (F.col(id).isNotNull(),
                                              F.col
                                              (id).alias
                                              (memberID)
                                              ).otherwise(F.lit(None))
                                             .alias(memberID)))
                # -------------------------------------------------------------
                # out accumulatedProperties
                # -------------------------------------------------------------
                columns = [F.col(attr + "." + c) for c in attrDict[memberID]]
                if "accumulatedProperties" in out_motifs[k].columns:
                    s_fields = out_motifs[k] \
                        .schema["accumulatedProperties"] \
                        .dataType.names
                    out_motifs[k] = out_motifs[k] \
                        .withColumn("accumulatedProperties",
                                    F.struct(*([F.col("accumulatedProperties"
                                                      )[c]
                                                .alias(c) for c in s_fields]
                                               + [F.when
                                                  (F.struct(columns)
                                                   .isNotNull(),
                                                   F.struct(columns))
                                                   .otherwise
                                                   (F.lit(None))
                                                   .alias(memberID)]
                                               )))
                else:
                    out_motifs[k] = out_motifs[k] \
                        .withColumn("accumulatedProperties",
                                    F.struct(F.when
                                             (F.struct(columns)
                                              .isNotNull(),
                                              F.struct(columns)
                                              ).otherwise(F.lit(None))
                                             .alias(memberID)))
            else:
                out_motifs[k] = out_motifs[k] \
                    .withColumn("memberID", F.struct(F.lit("")))
                out_motifs[k] = out_motifs[k] \
                    .withColumn("accumulatedProperties",
                                F.struct(F.lit("")))
    # -------------------------------------------------------------------------
    # remove empty search results from init_motifs
    # -------------------------------------------------------------------------
    removeList = []
    for k in out_motifs:
        if out_motifs[k].schema["memberID"].dataType.names[0] == 'col1':
            removeList.append(k)
    for i in removeList:
        del out_motifs[i]
    removeList = []
    for k in in_motifs:
        if in_motifs[k].schema["memberID"].dataType.names[0] == 'col1':
            removeList.append(k)
    for i in removeList:
        del out_motifs[i]
    # -------------------------------------------------------------------------
    # l2Members DF in init_motifs
    # -------------------------------------------------------------------------
    for k in out_motifs:
        out_motifs[k] = out_motifs[k].withColumn("groupactivecode",
                                                 F.lit(""))
        out_motifs[k] = out_motifs[k].withColumn("groupactivetimestamp",
                                                 F.lit(""))
        out_motifs[k] = out_motifs[k].select("start.groupID",
                                             "groupactivecode",
                                             "groupactivetimestamp",
                                             "memberID",
                                             "accumulatedProperties")
    for k in in_motifs:
        in_motifs[k] = in_motifs[k].withColumn("groupactivecode",
                                               F.lit(""))
        in_motifs[k] = in_motifs[k].withColumn("groupactivetimestamp",
                                               F.lit(""))
        in_motifs[k] = in_motifs[k].select("start.groupID",
                                           "groupactivecode",
                                           "groupactivetimestamp",
                                           "memberID",
                                           "accumulatedProperties")
        # ---------------------------------------------------------------------
        # change struct to map types
        # ---------------------------------------------------------------------
        # init_motifs[k] = init_motifs[k].rdd.map(lambda row: row.asDict
        #                                        (recursive=True)
        #                                        ).toDF(sampleRatio=0.08)

    # -------------------------------------------------------------------------
    # mirror schema of max init_motifs
    # -------------------------------------------------------------------------
    # klist = []
    # [klist.append(k) for k in init_motifs]
    # kmax = max(klist)
    # for k in init_motifs:
    #    init_motifs[k] = mirrorSchema(init_motifs[k], init_motifs[kmax])
    return out_motifs, in_motifs


def findMembers2(graph, length, startname):
    """
    FUNCTION: findMembers

    ARGUMENTS:
        # ---------------------------------------------------------------------
        # ARGUMENTS      | TYPES             | DESCRIPTION
        # ---------------------------------------------------------------------
        # graph          | graphFrame graph  | graph
        # ---------------------------------------------------------------------
        # length         | long()            | the range of the search motifs
        # ---------------------------------------------------------------------
        # startname      | string()          | filter node to start search from
        #----------------------------------------------------------------------

    DESCRIPTION:

    RETURNS:
        pyspark dataframe with groupID, memberID with found connected
        Members.

        root
         |-- groupID: string (nullable = true)
         |-- groupactivecode: string (nullable = false)
         |-- groupactivetimestamp: string (nullable = false)
         |-- memberID: struct (nullable = false)
         |    |-- HLITrade: string (nullable = true)
         |    |-- HLIFulfillment: string (nullable = true)
         |    |-- FactoryDelivery: string (nullable = true)

    EXAMPLES:
         +-------+-------------------------+------------------------------+
         |groupID                          | memberID                     |
         +-------+-------------------------+------------------------------+
         |300-7100002695000050-BridgeDirect|[7100002695000050, 4066166...]|
         |300-7200000388000080-BridgeDirect|[7200000388000080, 4067035...]|
         |300-7200000376000100-BridgeDirect|[7200000376000100, 4067048...]|
         +---------------------------------+------------------------------+
          only showing top 3 rows
    """
    # -------------------------------------------------------------------------
    # setup & default variables
    # -------------------------------------------------------------------------
    out_motifs = {}
    vCollect = graph \
        .vertices \
        .withColumn("groupID",
                    F.when(F.col("name") == startname,
                           F.concat_ws("-",
                                       "attributes.client",
                                       "id",
                                       "attributes.flowtype"))
                    ).select("id", "name", "groupID")
    eCollect = graph.edges
    # -------------------------------------------------------------------------
    # gCollect graph
    # -------------------------------------------------------------------------
    gCollect = GraphFrame(vCollect, eCollect)
    # -------------------------------------------------------------------------
    # dynamic edges out search motif
    # -------------------------------------------------------------------------
    for leng in reversed(range(length)):
        motifPath = createMotif(leng)
        membersMotifs = gCollect.find(motifPath
                                      ).where(F.col("start.name"
                                                    ) == startname)
        out_motifs[leng] = membersMotifs
    # -------------------------------------------------------------------------
    # list of names & ids from motif output
    # -------------------------------------------------------------------------
    for k in out_motifs:
        clist = list(enumerate(out_motifs[k].columns, 1))
        for v in clist:
            name = v[1] + ".name"
            id = v[1] + ".id"
            # -------------------------------------------------------------
            # out memberIDs
            # -------------------------------------------------------------
            if "memberID" in out_motifs[k].columns:
                s_fields = out_motifs[k].schema["memberID"].dataType.names
                out_motifs[k] = out_motifs[k] \
                    .withColumn("memberID",
                                [([F.col('memberID')[c].alias(c)
                                   for c in s_fields]
                                  + [F.create_map(F.col(name),
                                                  F.col(id))])])
            else:
                out_motifs[k] = out_motifs[k] \
                    .withColumn("memberID",
                                [F.when
                                 (F.col(id).isNotNull(),
                                  F.create_map(F.col(name),
                                               F.col(id))).otherwise
                                 (F.lit(None))])
    # -------------------------------------------------------------------------
    # l2Members DF in init_motifs
    # -------------------------------------------------------------------------
    for k in out_motifs:
        out_motifs[k] = out_motifs[k].select("start.groupID",
                                             "memberID")
    # -------------------------------------------------------------------------
    # remove empty search results from init_motifs
    # -------------------------------------------------------------------------
    removeList = []
    for k in out_motifs:
        if len(out_motifs[k].head(1)) == 0:
            removeList.append(k)
    for i in removeList:
        del out_motifs[i]
    # -------------------------------------------------------------------------
    return out_motifs
