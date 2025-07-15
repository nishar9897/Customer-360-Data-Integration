#!/bin/bash

# Set your project ID if needed
# gcloud config set project YOUR_PROJECT_ID

echo "Running Query 1: AOV_Product_Category_Location"
bq query --use_legacy_sql=false "
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
"
echo "Query 1 completed."

echo "Running Query 2: Customer_Segmentation"
bq query --use_legacy_sql=false "
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
"
echo "Query 2 completed."

echo "Running Query 3: Peak_DayHour_By_Channel"
bq query --use_legacy_sql=false "
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
"
echo "Query 3 completed."

echo "Running Query 4: Agent_Interaction_Summary"
bq query --use_legacy_sql=false "
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
"
echo "Query 4 completed."

echo "All gold layer aggregations are done!"
