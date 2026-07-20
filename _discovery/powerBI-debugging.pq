// Edges table – NodeIds are fully-qualified from source (SUB|RG|ADF|TYPE|NAME)
let
    Source = Excel.Workbook(File.Contents("C:\Users\CoreyCrumbaugh\OneDrive - Lantern\Documents\1-projects\guidehouse\code\adf_lineage_edges.xlsx"), null, true),
    adf_lineage_edges_Sheet = Source{[Item="adf_lineage_edges",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(adf_lineage_edges_Sheet, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"SubscriptionId", type text}, {"ResourceGroupName", type text}, {"DataFactoryName", type text}, {"SourceType", type text}, {"SourceNodeId", type text}, {"SourceName", type text}, {"RelationshipType", type text}, {"TargetType", type text}, {"TargetNodeId", type text}, {"TargetName", type text}, {"Notes", type text}})
in
    #"Changed Type"


// example in Nodes table I need to match in Edges

let
    Source = Table.Combine({Nodes_FromInventory, Nodes_FromEdges_Source, Nodes_FromEdges_Target}),

    #"Filtered NonBlank NodeId" =
        Table.SelectRows(
            Source,
            each [NodeId] <> null and [NodeId] <> ""
        ),

    #"Distinct NodeId" =
        Table.Distinct(
            #"Filtered NonBlank NodeId",
            {"NodeId"}
        )
in
    #"Distinct NodeId"

//Nodes
let
    Source = Table.Combine({Nodes_FromInventory, Nodes_FromEdges_Source, Nodes_FromEdges_Target}),

    #"Filtered NonBlank NodeId" =
        Table.SelectRows(
            Source,
            each [NodeId] <> null and [NodeId] <> ""
        ),

    #"Distinct NodeId" =
        Table.Distinct(
            #"Filtered NonBlank NodeId",
            {"NodeId"}
        )
in
    #"Distinct NodeId"



//Nodes_FromEdges_Source
let
    Source = Edges,
    #"Selected Columns" = Table.SelectColumns(Source, {"SourceNodeId"}),
    #"Renamed to NodeId" = Table.RenameColumns(#"Selected Columns", {{"SourceNodeId", "NodeId"}}),
    #"Distinct" = Table.Distinct(#"Renamed to NodeId")
in
    #"Distinct"


//Nodes_FromEdges_Target
let
    Source = Edges,
    #"Selected Columns" = Table.SelectColumns(Source, {"TargetNodeId"}),
    #"Renamed to NodeId" = Table.RenameColumns(#"Selected Columns", {{"TargetNodeId", "NodeId"}}),
    #"Distinct" = Table.Distinct(#"Renamed to NodeId")
in
    #"Distinct"

//Edges_Orphans_Source
let
    Source = Edges,
    Merge = Table.NestedJoin(Source, {"SourceNodeId"}, Nodes, {"NodeId"}, "n", JoinKind.LeftOuter),
    Expand = Table.ExpandTableColumn(Merge, "n", {"NodeId"}, {"MatchedNodeId"}),
    Orphans = Table.SelectRows(Expand, each [MatchedNodeId] = null or [MatchedNodeId] = "")
in
    Orphans


//Edges_Orphans_Target
let
    Source = Edges,
    Merge = Table.NestedJoin(Source, {"TargetNodeId"}, Nodes, {"NodeId"}, "n", JoinKind.LeftOuter),
    Expand = Table.ExpandTableColumn(Merge, "n", {"NodeId"}, {"MatchedNodeId"}),
    Orphans = Table.SelectRows(Expand, each [MatchedNodeId] = null or [MatchedNodeId] = "")
in
    Orphans

//Inventory
let
    Source = Excel.Workbook(File.Contents("C:\Users\CoreyCrumbaugh\OneDrive - Lantern\Documents\1-projects\guidehouse\code\adf_inventory.xlsx"), null, true),
    adf_inventory_Sheet = Source{[Item="adf_inventory",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(adf_inventory_Sheet, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"SubscriptionId", type text}, {"ResourceGroupName", type text}, {"DataFactoryName", type text}, {"ArtifactCategory", type text}, {"ArtifactName", type text}, {"ArtifactType", type text}, {"RelatedArtifacts", type text}, {"PropertiesJson", type any}}),
    #"Added NodeId" =
    Table.AddColumn(
        #"Changed Type",
        "NodeId",
        each
            Text.Combine(
                {
                    Text.From([SubscriptionId]),
                    Text.From([ResourceGroupName]),
                    Text.From([DataFactoryName]),
                    "Activity",
                    Text.From([ActivityPipelineName]) & "::" & Text.From([ArtifactName])
                },
                "|"
                ),
        type text
    ),

    #"Added ActivityPipelineName" =
        Table.AddColumn(
            #"Added NodeId",
            "ActivityPipelineName",
            each
                if [ArtifactCategory] = "Activity" then
                    let
                        ra = try Text.From([RelatedArtifacts]) otherwise "",
                        pipe = if ra = "" then null else try Text.AfterDelimiter(ra, "Pipeline=") otherwise null
                    in
                        pipe
                else
                    null,
            type text
        ),

    #"Rebuilt NodeId" =
        Table.AddColumn(
            #"Added ActivityPipelineName",
            "NodeId_New",
            each
                if [ArtifactCategory] = "Activity"
                    and [ActivityPipelineName] <> null
                    and Text.Trim([ActivityPipelineName]) <> ""
                then
                    Text.Clean(
                        Text.Upper(
                            Text.Combine(
                                {
                                    Text.From([SubscriptionId]),
                                    Text.From([ResourceGroupName]),
                                    Text.From([DataFactoryName]),
                                    "Activity",
                                    Text.From([ActivityPipelineName]) & "::" & Text.From([ArtifactName])
                                },
                                "|"
                            )
                        )
                    )
                else
                    Text.Clean(
                        Text.Upper(
                            Text.Combine(
                                {
                                    Text.From([SubscriptionId]),
                                    Text.From([ResourceGroupName]),
                                    Text.From([DataFactoryName]),
                                    Text.From([ArtifactCategory]),
                                    Text.From([ArtifactName])
                                },
                                "|"
                            )
                        )
                    ),
            type text
        ),

    #"Removed Old NodeId" =
        Table.RemoveColumns(
            #"Rebuilt NodeId",
            {"NodeId"}
        ),

    #"Renamed NodeId" =
        Table.RenameColumns(
            #"Removed Old NodeId",
            {{"NodeId_New", "NodeId"}}
        )
in
    #"Renamed NodeId"