aws dynamodb create-table \
    --table-name stock_eod \
    --attribute-definitions \
        AttributeName=stockTicker,AttributeType=S AttributeName=tradeDate,AttributeType=S \
    --key-schema AttributeName=stockTicker,KeyType=HASH AttributeName=tradeDate,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    --region us-east-1

aws dynamodb put-item \
    --table-name stock_eod \
    --item '{ 
        "stockTicker": {"S": "A"}, 
        "tradeDate": {"S": "01-Jan-2009"}, 
        "lowPrice": {"N": "35"}, 
        "openPrice": {"N": "35"}, 
        "highPrice": {"N": "35"}, 
        "closePrice": {"N": "35"}, 
        "volume": {"N": "0"} }' \
    --return-consumed-capacity TOTAL \
    --region us-east-1

aws dynamodb put-item \
    --table-name stock_eod \
    --item '{ 
        "stockTicker": {"S": "A"}, 
        "tradeDate": {"S": "02-Jan-2009"}, 
        "lowPrice": {"N": "35"}, 
        "openPrice": {"N": "35"}, 
        "highPrice": {"N": "35"}, 
        "closePrice": {"N": "35"}, 
        "volume": {"N": "0"} }' \
    --return-consumed-capacity TOTAL \
    --region us-east-1

aws dynamodb query --table-name stock_eod --key-conditions file:///Users/usdgadiraj/code/dynamoquery.json --region us-east-1

aws dynamodb query --table-name stock_eod --region us-east-1 --key-conditions '{
    "stockTicker": {
        "AttributeValueList": [
            {   
                "S": "A"
            }   
        ],  
        "ComparisonOperator": "EQ"
    },  
    "tradeDate": {
        "AttributeValueList": [
            {   
                "S": "01-Jan-2009"
            }   
        ],  
        "ComparisonOperator": "EQ"
    }
}'

-- select * from stock_eod;
aws dynamodb scan --table-name stock_eod --region us-east-1 

-- select * from stock_eod limit 10;
-- select * from stock_eod where rownum <= 10;
aws dynamodb scan     --table-name stock_eod     --region us-east-1     --limit 10 

-- select count(1) from stock_eod;
aws dynamodb scan     --table-name stock_eod     --region us-east-1     --select COUNT

-- stockTicker, tradeDate, lp, op, cp, hp, v
aws dynamodb scan \
     --table-name stock_eod \
     --region us-east-1 \
     --select SPECIFIC_ATTRIBUTES \
     --attributes-to-get "v" \
     --limit 10

-- select tradeDate, stockTicker, v from stock_eod limit 10;
aws dynamodb scan \
     --table-name stock_eod \
     --region us-east-1 \
     --select SPECIFIC_ATTRIBUTES \
     --projection-expression "tradeDate,stockTicker,v" \
     --limit 10 

-- drop table stock_eod;
aws dynamodb delete-table     --table-name stock_eod    --region us-east-1
