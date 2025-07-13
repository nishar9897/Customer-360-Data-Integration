from google.cloud import bigquery

# You can set the project or leave it to use default project
client = bigquery.Client()

# Query 1: Average Order Value per Product/Category
query1 = """
CREATE OR REPLACE TABLE goldlayer.AOV_Product_Category_Location AS
SELECT
  o.ProductID,
  p.Name AS ProductName,
  p.Category,
  NULL AS Location,
  SUM(o.Amount) AS TotalAmount,
  COUNT(o.OrderID) AS TotalOrders,
  SAFE_DIVIDE(SUM(o.Amount), COUNT(o.OrderID)) AS AverageOrderValue
FROM silverlayer.OnlineTransactions o
LEFT JOIN silverlayer.Products p ON o.ProductID = p.ProductID
GROUP BY o.ProductID, ProductName, p.Category;
"""

# Query 2: Customer Segmentation
query2 = """
CREATE OR REPLACE TABLE goldlayer.Customer_Segmentation AS
WITH all_txns AS (
  SELECT CustomerID, Amount FROM silverlayer.OnlineTransactions
  UNION ALL
  SELECT CustomerID, Amount FROM silverlayer.InStoreTransactions
),
agg AS (
  SELECT
    CustomerID,
    SUM(Amount) AS TotalSpend,
    COUNT(*) AS PurchaseFrequency
  FROM all_txns
  GROUP BY CustomerID
),
percentiles AS (
  SELECT
    *,
    NTILE(10) OVER (ORDER BY TotalSpend DESC) AS SpendDecile
  FROM agg
),
loyalty AS (
  SELECT CustomerID, TierLevel FROM silverlayer.LoyaltyAccounts
)
SELECT
  p.CustomerID,
  p.TotalSpend,
  p.PurchaseFrequency,
  l.TierLevel,
  CASE
    WHEN p.SpendDecile = 1 THEN 'High-Value Customers'
    WHEN p.PurchaseFrequency = 1 THEN 'One-Time Buyers'
    WHEN l.TierLevel IN ('Gold','Platinum','Diamond','Elite') THEN 'Loyalty Champions'
    ELSE 'Regular'
  END AS Segment
FROM percentiles p
LEFT JOIN loyalty l ON p.CustomerID = l.CustomerID;
"""

# Query 3: Peak Day/Hour By Channel
query3 = """
CREATE OR REPLACE TABLE goldlayer.Peak_DayHour_By_Channel AS
SELECT
  'Online' AS Channel,
  EXTRACT(DAYOFWEEK FROM DateTime) AS DayOfWeek,
  EXTRACT(HOUR FROM DateTime) AS HourOfDay,
  COUNT(*) AS NumTransactions
FROM silverlayer.OnlineTransactions
GROUP BY Channel, DayOfWeek, HourOfDay

UNION ALL

SELECT
  'InStore' AS Channel,
  EXTRACT(DAYOFWEEK FROM DateTime) AS DayOfWeek,
  EXTRACT(HOUR FROM DateTime) AS HourOfDay,
  COUNT(*) AS NumTransactions
FROM silverlayer.InStoreTransactions
GROUP BY Channel, DayOfWeek, HourOfDay
ORDER BY Channel, NumTransactions DESC;
"""

# Query 4: Agent Interaction Summary
query4 = """
CREATE OR REPLACE TABLE goldlayer.Agent_Interaction_Summary AS
SELECT
  a.AgentID,
  ag.Name AS AgentName,
  COUNT(*) AS NumInteractions,
  SUM(CASE WHEN a.ResolutionStatus = 'Resolved' THEN 1 ELSE 0 END) AS NumResolved,
  SAFE_DIVIDE(SUM(CASE WHEN a.ResolutionStatus = 'Resolved' THEN 1 ELSE 0 END), COUNT(*)) AS ResolutionRate
FROM
  silverlayer.CustomerServiceInteractions a
LEFT JOIN
  silverlayer.Agents ag ON a.AgentID = ag.AgentID
GROUP BY
  a.AgentID, AgentName
ORDER BY
  NumInteractions DESC;
"""

for i, q in enumerate([query1, query2, query3, query4], start=1):
    print(f"Running query {i}...")
    client.query(q).result()
    print(f"Query {i} completed.")

print("All aggregations done! Gold layer tables are created.")
