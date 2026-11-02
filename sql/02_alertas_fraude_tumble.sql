-- Detecção de anomalia de volume por conta em janelas móveis de 1 minuto (Flink SQL TVF)
SELECT 
    window_start AS inicio_janela,
    window_end AS fim_janela,
    JSON_VALUE(CAST(val AS STRING), '$.after.conta_id') AS id_conta,
    COUNT(*) AS total_transacoes
FROM TABLE(
    TUMBLE(
        TABLE `default`.`desafio-basic`.`postgres.public.transacoes`,
        DESCRIPTOR(`$rowtime`),
        INTERVAL '1' MINUTE
    )
)
GROUP BY 
    window_start, 
    window_end, 
    JSON_VALUE(CAST(val AS STRING), '$.after.conta_id')
HAVING COUNT(*) >= 2;