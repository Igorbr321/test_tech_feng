SELECT
    DATE_FORMAT(BC.DATA_CANCELAMENTO, '%m') AS MES,
    COUNT(*) AS PARCELAS_CANCELADAS,
    SUM(BC.VALOR_COBRANCA) AS VALOR_TOTAL_CANCELADO
FROM 
    BASE_COBRANCAS BC
WHERE 
    BC.DATA_CANCELAMENTO IS NOT NULL -- Parcelas canceladas
    AND YEAR(BC.DATA_CANCELAMENTO) = 2022 -- Apenas 2022
GROUP BY 
    DATE_FORMAT(BC.DATA_CANCELAMENTO, '%m')
ORDER BY 
    PARCELAS_CANCELADAS DESC
LIMIT 1; -- Retorna o mês com mais cancelamentos