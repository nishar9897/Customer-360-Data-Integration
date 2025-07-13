# Load Customers table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=CustomerID:INT64,Name:STRING,Email:STRING,Address:STRING \
  silverlayer.Customers \
  gs://cleaned_silverlayer/Customers_cleaned.csv/*

# Load Products table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=ProductID:INT64,Name:STRING,Category:STRING,Price:NUMERIC \
  silverlayer.Products \
  gs://cleaned_silverlayer/Products_cleaned.csv/*

# Load OnlineTransactions table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=OrderID:INT64,CustomerID:INT64,ProductID:INT64,DateTime:DATETIME,PaymentMethod:STRING,Amount:NUMERIC,Status:STRING \
  silverlayer.OnlineTransactions \
  gs://cleaned_silverlayer/OnlineTransactions_cleaned.csv/*

# Load Stores table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=StoreID:INT64,Location:STRING,Manager:STRING,OpenHours:STRING \
  silverlayer.Stores \
  gs://cleaned_silverlayer/Stores_cleaned.csv/*

# Load InStoreTransactions table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=TransactionID:INT64,CustomerID:INT64,StoreID:INT64,DateTime:DATETIME,Amount:NUMERIC,PaymentMethod:STRING \
  silverlayer.InStoreTransactions \
  gs://cleaned_silverlayer/InStoreTransactions_cleaned.csv/*

# Load Agents table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=AgentID:INT64,Name:STRING,Department:STRING,Shift:STRING \
  silverlayer.Agents \
  gs://cleaned_silverlayer/Agents_cleaned.csv/*

# Load CustomerServiceInteractions table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=InteractionID:INT64,CustomerID:INT64,DateTime:DATETIME,AgentID:INT64,IssueType:STRING,ResolutionStatus:STRING \
  silverlayer.CustomerServiceInteractions \
  gs://cleaned_silverlayer/CustomerServiceInteractions_cleaned.csv/*

# Load LoyaltyAccounts table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=LoyaltyID:INT64,CustomerID:INT64,PointsEarned:INT64,TierLevel:STRING,JoinDate:DATE \
  silverlayer.LoyaltyAccounts \
  gs://cleaned_silverlayer/LoyaltyAccounts_cleaned.csv/*

# Load LoyaltyTransactions table
bq load --skip_leading_rows=1 --source_format=CSV \
  --schema=LoyaltyID:INT64,DateTime:DATETIME,PointsChange:INT64,Reason:STRING \
  silverlayer.LoyaltyTransactions \
  gs://cleaned_silverlayer/LoyaltyTransactions_cleaned.csv/*
