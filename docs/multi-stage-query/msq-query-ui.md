---
id: query-ui
title: UI walkthrough
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

The Multi-Stage Query (MSQ) Framework uses the enhanced **Query** view, which provides you with a UI to edit and use SQL queries. If MSQ is enabled, the enhanced **Query** view is available automatically. No additional configuration is required.

The following screenshot shows you a populated enhanced **Query** view along with a description of its parts:

![Annotated multi-stage query view](../assets/multi-stage-query/ui-annotated.png)

1. The multi-stage, tab-enabled, **Query** view is where all the new functionality is contained.
All other views are unchanged from the non-enhanced version. You can still access the original Query view by navigating to `#query` in the URL.
You can tell that you're looking at the MSQ capable **Query** view by the presence of the tabs (3).
2. The **Resources** view shows the available schemas, datasources, and columns.
3. Query tabs allow you to manage and run several queries at once.
Click the **+** button to open a new tab.
Existing tabs can be manipulated by clicking the tab name.
4. The tab bar contains some helpful tools including a **Connect external data** button that samples external data and creates an initial query with the appropriate `EXTERN` definition that you can then edit as needed.
5. The **Work history** panel lets you see currently running and previous queries from all users in the cluster.
It is equivalent to the **Task** view in the **Ingestion** view with the filter of `type='query_controller'`.
6. You can click on each query entry to attach to that query in a new tab to track its progress if it is currently running or view its result if it finished.
7. You can download an archive of all the pertinent details about the query that you can share.
8. The **Run** button runs the query.
9. The **Preview** button appears when you enter an INSERT/REPLACE query. It runs the query inline without the INSERT/REPLACE clause and with an added LIMIT to give you a preview of the data that would be ingested if you click **Run**.
The added limits make the query run faster but provides incomplete results.
10. The engine selector lets you choose which engine (API endpoint) to send a query to. By default, it automatically picks which endpoint to use based on an analysis of the query, but you can select a specific engine explicitly. You can also configure the engine specific context parameters from this menu.
11. The **Max tasks** picker appears when you have the **sql-task** engine selected. It lets you configure the degree of parallelism.
12. The More menu (**...**) contains helpful tools like:
- **Explain SQL query** shows you the logical plan returned by `EXPLAIN PLAN FOR` for a SQL query.
- **Query history** shows you previously executed queries.
- **Convert ingestion spec to SQL** lets you convert a native batch ingestion spec to an equivalent SQL query.
- **Attach tab from task ID** lets you create a new tab from the task ID of a query executed on this cluster.
- **Open query detail archive** lets you open a detail archive generated on any cluster by (7).
13. The query timer indicates how long the query has been running for.
14. The **(cancel)** link cancels the currently running query.
15. The main progress bar shows the overall progress of the query.
The progress is computed from the various counters in the live reports (16).
16. The **Current stage** progress bar shows the progress for the currently running query stage.
If several stages are executing concurrently, it conservatively shows the information for the earliest executing stage.
17. The live query reports show detailed information of all the stages (past, present, and future). The live reports are shown while the query is running. You can hide the report if you want.
After queries finish, they are available by clicking on the query time indicator or from the **Work history** panel (6).
18. Each stage of the live query reports can be expanded by clicking on the triangle to show per worker and per partition statistics.
