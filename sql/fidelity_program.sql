CREATE OR REPLACE TABLE `emporio-zingaro.z316_tiny.z316-fidelity_23Q4` AS
WITH 
TotalSpending AS (
  SELECT
    cpfCnpj,
    SUM(totalVenda) AS total_spending
  FROM
    `emporio-zingaro.z316_tiny.z316-tiny-pedidos`
  WHERE
    data_pedido BETWEEN '2023-10-01' AND '2023-12-31' AND
    cpfCnpj IS NOT NULL AND
    nome <> 'Consumidor Final' AND
    formaPagamento IN ('credito', 'debito', 'pix', 'multiplas', 'dinheiro') AND
    desconto IN ('0', '0.0', '0.00')
  GROUP BY
    cpfCnpj
),
TrimesterSpending AS (
  SELECT
    cpfCnpj,
    SUM(totalVenda) AS trimester_spending
  FROM
    `emporio-zingaro.z316_tiny.z316-tiny-pedidos`
  WHERE
    data_pedido BETWEEN '2023-10-01' AND '2023-12-31' AND
    cpfCnpj IS NOT NULL AND
    nome <> 'Consumidor Final' AND
    formaPagamento IN ('credito', 'debito', 'pix', 'multiplas', 'dinheiro') AND
    desconto IN ('0', '0.0', '0.00')
  GROUP BY
    cpfCnpj
),
DailyCheckIns AS (
  SELECT
    cpfCnpj,
    COUNT(DISTINCT DATE(data_pedido)) AS daily_check_ins
  FROM
    `emporio-zingaro.z316_tiny.z316-tiny-pedidos`
  WHERE
    data_pedido BETWEEN '2023-10-01' AND '2023-12-31' AND
    cpfCnpj IS NOT NULL AND
    nome <> 'Consumidor Final'
  GROUP BY
    cpfCnpj
),
CombinedMetrics AS (
  SELECT
    t.cpfCnpj,
    t.total_spending,
    ts.trimester_spending,
    d.daily_check_ins
  FROM
    TotalSpending t
  JOIN
    TrimesterSpending ts ON t.cpfCnpj = ts.cpfCnpj
  JOIN
    DailyCheckIns d ON t.cpfCnpj = d.cpfCnpj
),
CumulativeCategories AS (
    SELECT
        cpfCnpj,
        total_spending,
        trimester_spending,
        daily_check_ins,
        SUM(total_spending) OVER (ORDER BY total_spending DESC) AS cumulative_total_spending,
        SUM(trimester_spending) OVER (ORDER BY trimester_spending DESC) AS cumulative_trimester_spending,
        SUM(daily_check_ins) OVER (ORDER BY daily_check_ins DESC) AS cumulative_daily_check_ins
    FROM
        CombinedMetrics
),
OverallTotals AS (
    SELECT
        SUM(total_spending) AS overall_total_spending,
        SUM(trimester_spending) AS overall_trimester_spending,
        SUM(daily_check_ins) AS overall_daily_check_ins
    FROM
        CombinedMetrics
),
ProportionalMetrics AS (
    SELECT
        cc.cpfCnpj,
        cc.total_spending,
        cc.trimester_spending,
        cc.daily_check_ins,
        cc.cumulative_total_spending / ot.overall_total_spending AS prop_total_spending,
        cc.cumulative_trimester_spending / ot.overall_trimester_spending AS prop_trimester_spending,
        cc.cumulative_daily_check_ins / ot.overall_daily_check_ins AS prop_daily_check_ins
    FROM
        CumulativeCategories cc
    CROSS JOIN
        OverallTotals ot
),
Rankings AS (
  SELECT
    cpfCnpj,
    CASE
        WHEN prop_total_spending <= 0.5 THEN 'A'
        WHEN prop_total_spending <= 0.8 THEN 'B'
        ELSE 'C'
    END AS total_spending_rank,
    CASE
        WHEN prop_trimester_spending <= 0.5 THEN 'A'
        WHEN prop_trimester_spending <= 0.8 THEN 'B'
        ELSE 'C'
    END AS trimester_spending_rank,
    CASE
        WHEN prop_daily_check_ins <= 0.5 THEN 'A'
        WHEN prop_daily_check_ins <= 0.8 THEN 'B'
        ELSE 'C'
    END AS check_in_rank
  FROM
    ProportionalMetrics
)
SELECT
  c.nome,
  c.cpf_cnpj,
  c.email,
  '2023 Q4' AS season,
  m.total_spending,
  m.trimester_spending,
  m.daily_check_ins,
  r.total_spending_rank,
  r.trimester_spending_rank,
  r.check_in_rank,
CASE
  WHEN r.trimester_spending_rank = 'A' AND r.check_in_rank IN ('A', 'B') THEN 'Platinum'
  WHEN r.trimester_spending_rank = 'A' OR r.check_in_rank = 'A' OR (r.trimester_spending_rank = 'B' AND r.check_in_rank = 'B') THEN 'Gold'
  WHEN r.trimester_spending_rank = 'B' OR r.check_in_rank = 'B' THEN 'Silver'
  ELSE 'Bronze'
END AS final_tier
###### code for following cycles
###### --> https://platform.openai.com/playground/p/dWiHQJvfTg3PIVDLEEVzj6fE?model=gpt-4-1106-preview&mode=chat
#CASE
#  WHEN r.total_spending_rank = 'A' AND r.trimester_spending_rank >= 'B' THEN 'Platinum'
#  WHEN r.trimester_spending_rank = 'A' OR r.check_in_rank = 'A' OR (r.trimester_spending_rank = 'B' AND r.check_in_rank = 'B') THEN 'Gold'
#  WHEN r.total_spending_rank = 'B' OR r.trimester_spending_rank = 'B' OR r.check_in_rank = 'B' THEN 'Silver'
#  ELSE 'Bronze'
#END AS final_tier
FROM
  `emporio-zingaro.z316_tiny.z316-tiny-contatos` c
JOIN 
  ProportionalMetrics m ON c.cpf_cnpj = m.cpfCnpj AND REGEXP_CONTAINS(c.cpf_cnpj, r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')
JOIN 
  Rankings r ON c.cpf_cnpj = r.cpfCnpj;
