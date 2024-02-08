# Empório Zingaro Loyalty Program

## Overview
This repository hosts SQL and Python scripts for implementing Empório Zingaro's Fidelity Program, focusing on customer engagement metrics to assign loyalty tiers and calculate commissions and cashbacks for sales based on these tiers.

## Table of Contents
- [Overview](#overview)
- [Program Logic](#program-logic)
  - [Engagement Metrics and ABC Ranking System](#engagement-metrics-and-abc-ranking-system)
  - [Tier Allocation Logic](#tier-allocation-logic)
- [SQL Script Breakdown](#sql-script-breakdown)
  - [Fidelity Program](#fidelity-program)
  - [Commission Details](#commission-details)
  - [Commission Summary](#commission-summary)
- [Running the Scripts](#running-the-scripts)
- [Contributing](#contributing)
- [Future Improvements](#future-improvements)
- [Contact](#contact)

## Program Logic

### Engagement Metrics and ABC Ranking System
The program uses Daily Check-ins, Seasonal Spend, and Lifetime Spend as key metrics. These are calculated as follows:

- **Daily Check-ins**: Counts the distinct days a customer has made at least one purchase, emphasizing regular engagement.
- **Seasonal Spend**: Aggregates purchases within a specified three-month period, reflecting recent customer activity.
- **Lifetime Spend**: Sums all purchases by a customer since the loyalty program began, measuring long-term engagement.

Customers are ranked into A, B, or C categories using the ABC ranking system, which applies the Pareto principle:
- **Rank A**: Assigned to the top 20% of customers based on engagement or spending, indicating highest loyalty.
- **Rank B**: Represents the next 30%, showing significant engagement.
- **Rank C**: Includes the remaining 50%, acknowledging their contribution to the store's ecosystem.

### Tier Allocation Logic
The assignment of loyalty tiers is directly based on the ABC ranking derived from the metrics calculated in the SQL script. The logic encoded in the SQL statements determines the customer's tier as follows:

- **Platinum Tier**: Assigned to customers who achieve an 'A' rank in Lifetime Spend, demonstrating significant long-term loyalty, and at least a 'B' rank in Seasonal Spend, indicating good recent engagement. This tier is for those who consistently contribute to the top percentile of store engagement and spending.
 
- **Gold Tier**: Customers receive this tier if they have an 'A' rank in either Seasonal Spend or Daily Check-ins, highlighting recent active engagement, or 'B' ranks in both metrics, demonstrating consistent engagement and spending. It's designed for customers who are highly engaged in the recent term but might not yet have the long-term interaction of Platinum members.
 
- **Silver Tier**: This tier is for customers with 'B' ranks in either the Lifetime Spend, Seasonal Spend, or Daily Check-ins metrics. It acknowledges customers who are actively engaged and contribute significantly to the store's ecosystem but at a level below that of Gold and Platinum tiers.

- **Bronze Tier**: Automatically assigned to customers who do not meet the criteria for the other tiers, the Bronze tier ensures every participant in the loyalty program is recognized. It's a foundational tier that encompasses a broad spectrum of the customer base, from occasional shoppers to new members of the program.

The SQL logic uses conditional statements to evaluate each customer's ranks across the metrics and assign the appropriate tier. The 'ELSE Bronze' condition in the tier assignment logic ensures inclusivity by defaulting to Bronze for customers who do not fit into the higher tiers, thus guaranteeing acknowledgment for all participants in the program.

## SQL Script Breakdown

### Fidelity Program
The `fidelity_program.sql` script operationalizes these concepts through a series of SQL queries:
1. **TotalSpending, TrimesterSpending, DailyCheckIns**: These Common Table Expressions (CTEs) compute the foundational metrics for each customer—Lifetime Spend, Seasonal Spend, and Daily Check-ins, respectively.
2. **CombinedMetrics**: Aggregates the above metrics for each customer, preparing the data for ranking analysis.
3. **CumulativeCategories and OverallTotals**: Calculate cumulative and total metrics to establish a basis for proportional comparisons, essential for the ABC ranking.
4. **ProportionalMetrics**: Determines the proportion of each customer's metrics relative to the total, applying the Pareto principle to categorize customers into A, B, or C ranks.
5. **Rankings**: Applies conditional logic to assign ABC ranks based on the proportional metrics, reflecting the customer's engagement and spending level.
6. **Final Tier Assignment**: Combines the ABC ranks for each criterion to allocate each customer to a loyalty tier, using a predefined logic that balances recent engagement with long-term loyalty.

### Commission Details
The `commission_details.sql` script calculates commissions and cashbacks for each sale, based on the customer's loyalty tier:

1. **filtered_pedidos**: Filters sales transactions within a specific period and ensures each sale is uniquely identified.
2. The `SELECT` statement then calculates commissions and cashbacks by applying predefined percentages to the sales amount, differing by customer tier:
   - Platinum tier leads to the lowest commission but highest cashback rates, incentivizing sales to top-tier customers.
   - The commission and cashback percentages increase for lower tiers, with Bronze tier transactions yielding the highest commissions for salespersons.

### Commission Summary
The `commission_summary.sql` script provides a summary of commissions earned by each salesperson over the specified period:

1. Uses the same `filtered_pedidos` CTE to filter and prepare sales data.
2. Aggregates commissions for each salesperson by summing up the commission amounts calculated for each sale, segmented by the customer's loyalty tier.
3. The `GROUP BY` clause ensures commissions are totaled per salesperson, providing a clear summary of earnings from sales to loyalty program members.

## Running the Scripts
Execute the SQL scripts within Google BigQuery, and after calculating the loyalty tiers, use `bq_to_sendgrid.py` to distribute program results to customers, detailing their tier and associated rewards.

## Future Improvements

The following items are identified as areas for enhancement in the future iterations of the Empório Zingaro Loyalty Program project:

- **Step-by-Step Usage Instructions**: Develop comprehensive, step-by-step guides detailing how to execute the SQL queries and utilize the Python script within the Google BigQuery environment, ensuring users of all skill levels can effectively interact with the program components.

- **Refactor Python Code for Configuration Management**: Update the Python script to leverage environment variables for configuration settings, such as API keys and database connection strings, enhancing security and flexibility in different environments.

- **Dynamic Date Handling in SQL Queries**: Modify the `fidelity_program.sql` script to dynamically calculate date ranges based on the current date, automatically targeting the previous trimester. This improvement aims to eliminate the need for manual date updates in the script, streamlining the process for end-of-trimester executions.

- **Scripted Query Execution**: Develop a Python script to encapsulate and run the SQL queries in sequence directly from a terminal. This enhancement aims to provide a streamlined, automated process for executing the loyalty program's data processing steps, reducing dependency on the Google BigQuery UI and enhancing usability for a broader range of users and use cases.

These enhancements are aimed at improving the usability, security, and automation of the loyalty program, aligning with best practices in software development and data engineering.

## Contributing
Contributions are invited to enhance the fidelity program's effectiveness and accuracy. Significant improvements may be rewarded with recognition within the program's structure.

## Contact
For more information or to contribute, please contact Rodrigo Brunale at [rodrigo@brunale.com](mailto:rodrigo@brunale.com).

