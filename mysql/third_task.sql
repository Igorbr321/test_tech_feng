WITH VALORES_PAGOS AS (
    SELECT
        BC.IDCONTRATO,
        SUM(BC.VALOR_COBRANCA) AS VALOR_TOTAL_PAGO
    FROM 
        BASE_COBRANCAS BC
    WHERE 
        BC.DATA_PAGAMENTO IS NOT NULL -- Cobranças pagas
    GROUP BY 
        BC.IDCONTRATO
    ORDER BY 
        VALOR_TOTAL_PAGO DESC
)
SELECT
    IDCONTRATO,
    VALOR_TOTAL_PAGO
FROM 
    VALORES_PAGOS
ORDER BY 
    VALOR_TOTAL_PAGO DESC
LIMIT 1 OFFSET 1; -- Pega o segundo maior valor