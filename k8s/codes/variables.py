PATH_SOURCE = 's3://landing-zone-vini-etl-aws/data/AdventureWorks/{file}.csv'
PATH_TARGET = 's3://processing-zone-vini-etl-aws/processing/{file}'
PATH_CURATED = 's3://curated-zone-vini-etl-aws/curated/'

VIEWS = [
  'Calendar',
  'Customers',
  'Product_Categories',
  'Product_Subcategories',
  'Products',
  'Returns',
  'Sales_2015',
  'Sales_2016',
  'Sales_2017'
]

QUERY = {
    
'QUERY': """ 
    WITH all_sales (
        SELECT * FROM Sales_2015
        UNION ALL
        SELECT * FROM Sales_2016
        UNION ALL
        SELECT * FROM Sales_2017
    ), info as (
    SELECT
        cast(from_unixtime(unix_timestamp(a.OrderDate, 'M/d/yyyy'), 'yyyy-MM-dd') as date) as OrderDate,
        cast(from_unixtime(unix_timestamp(a.StockDate, 'M/d/yyyy'), 'yyyy-MM-dd') as date) as StockDate,
        cast(a.CustomerKey as int) as CustomerKey,
        cast(a.TerritoryKey as int) as TerritoryKey,
        cast(a.OrderLineItem as int) as OrderLineItem,
        cast(a.OrderQuantity as int) as OrderQuantity,
        b.Prefix,
        b.FirstName,
        b.LastName,
        cast(from_unixtime(unix_timestamp(b.BirthDate, 'M/d/yyyy'), 'yyyy-MM-dd') as date) as BirthDate,
        b.MaritalStatus,
        b.Gender,
        b.EmailAddress,
        cast(replace(replace(b.AnnualIncome, "$", ""), ",", "") as decimal(10,2)) as AnnualIncome,
        cast(b.TotalChildren as int) as TotalChildren,
        b.EducationLevel,
        b.Occupation,
        b.HomeOwner,
        cast(c.ProductKey as int) as ProductKey,
        cast(d.ProductSubcategoryKey as int) as ProductSubcategoryKey,
        d.SubcategoryName,
        cast(d.ProductCategoryKey as int) as ProductCategoryKey,
        e.CategoryName,
        c.ProductSKU,
        c.ProductName,
        c.ModelName,
        c.ProductDescription,
        c.ProductColor,
        cast(c.ProductSize as int) as ProductSize,
        c.ProductStyle,
        cast(c.ProductCost as decimal(10,2)) as ProductCost ,
        cast(c.ProductPrice as decimal(10,2)) as ProductPrice,
        cast(from_unixtime(unix_timestamp(f.ReturnDate, 'M/d/yyyy'), 'yyyy-MM-dd') as date) as ReturnDate,
        NVL(cast(f.ReturnQuantity as int),0) as ReturnQuantity
    FROM
        all_sales a
    LEFT JOIN
        Customers b
    ON
        a.CustomerKey = b.CustomerKey
    LEFT JOIN
        Products c
    ON
        a.ProductKey = c.ProductKey
    LEFT JOIN
        Product_Subcategories d
    ON
        c.ProductSubcategoryKey = d.ProductSubcategoryKey
    LEFT JOIN
        Product_Categories e
    ON
        d.ProductCategoryKey = e.ProductCategoryKey
    LEFT JOIN
        Returns f
    ON
        a.TerritoryKey = f.TerritoryKey AND
        c.ProductKey = f.ProductKey
    )
    SELECT
        *
    FROM
        info
"""
}