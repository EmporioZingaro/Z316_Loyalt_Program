CREATE OR REPLACE TABLE emporio-zingaro.z316_tiny.z316_commission_details_23Q4 AS
WITH filtered_pedidos AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY pedido_number ORDER BY data_pedido) as rn
    FROM `emporio-zingaro.z316_tiny.z316-tiny-pedidos`
    WHERE
    data_pedido BETWEEN '2023-10-01' AND '2023-12-31' AND
    cpfCnpj IS NOT NULL AND
    nome <> 'Consumidor Final' AND
    formaPagamento IN ('credito', 'debito', 'pix', 'multiplas', 'dinheiro') AND
    desconto IN ('0', '0.0', '0.00')
)
SELECT 
    fp.data_pedido,
    fp.pedido_number,
    fp.nome,
    f.cpf_cnpj as cpf,
    f.final_tier,
    fp.id_vendedor,
    fp.nome_vendedor,
    fp.totalVenda,
    CASE f.final_tier
        WHEN 'Platinum' THEN fp.totalVenda * 0.01
        WHEN 'Gold' THEN fp.totalVenda * 0.02
        WHEN 'Silver' THEN fp.totalVenda * 0.03
        WHEN 'Bronze' THEN fp.totalVenda * 0.04
        ELSE 0 END AS commission,
    CASE f.final_tier
        WHEN 'Platinum' THEN fp.totalVenda * 0.05
        WHEN 'Gold' THEN fp.totalVenda * 0.03
        WHEN 'Silver' THEN fp.totalVenda * 0.02
        WHEN 'Bronze' THEN fp.totalVenda * 0.01
        ELSE 0 END AS cashback
FROM 
    filtered_pedidos fp
INNER JOIN 
    `emporio-zingaro.z316_tiny.z316-fidelity_23Q4` f ON fp.cpfCnpj = f.cpf_cnpj
WHERE 
    f.final_tier IS NOT NULL AND
    fp.cpfCnpj IS NOT NULL AND
    REGEXP_CONTAINS(f.cpf_cnpj, r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')
ORDER BY 
    fp.data_pedido, fp.pedido_number;
