-- OBS: Alguns dados precisarão de tratamento prévio no Banco de Dados por sua formatação como por exemplo: ''Terminal de AÃ§Ãºcar - TA (cadastro antigo)''
SELECT
    CASE
        WHEN PortoAtracacao IN (SELECT Porto FROM PortosCeara) THEN 'Ceará'
        WHEN PortoAtracacao IN (SELECT Porto FROM PortosNordeste) THEN 'Nordeste'
        ELSE 'Brasil'
    END AS Localidade,
    COUNT(IDAtracacao) AS Numero_Atracoes,
    AVG(DATEDIFF(MINUTE, DataChegada, DataAtracacao)) / 60.0 AS TempoEsperaHoras,
    AVG(DATEDIFF(MINUTE, DataAtracacao, DataDesatracacao)) / 60.0 AS TempoAtracadoHoras,
    MONTH(DataAtracacao) AS Mes,
    YEAR(DataAtracacao) AS Ano,
    (COUNT(IDAtracacao) - 
        (SELECT COUNT(IDAtracacao) 
         FROM Atracacao a2
         WHERE YEAR(a2.DataAtracacao) = 2022
         AND MONTH(a2.DataAtracacao) = MONTH(A.DataAtracacao)
         AND (PortoAtracacao IN (SELECT Porto FROM PortosCeara) OR 
              PortoAtracacao IN (SELECT Porto FROM PortosNordeste) OR
              PortoAtracacao IN (SELECT Porto FROM PortosBrasil)))) AS Variação_Numero_Atracacao_Bonus
FROM
    Atracacao A
WHERE
    YEAR(DataAtracacao) IN (2021, 2023)
    AND (PortoAtracacao IN (SELECT Porto FROM PortosCeara) OR
         PortoAtracacao IN (SELECT Porto FROM PortosNordeste) OR
         PortoAtracacao IN (SELECT Porto FROM PortosBrasil))
GROUP BY
    CASE
        WHEN PortoAtracacao IN (SELECT Porto FROM PortosCeara) THEN 'Ceará'
        WHEN PortoAtracacao IN (SELECT Porto FROM PortosNordeste) THEN 'Nordeste'
        ELSE 'Brasil'
    END,
    MONTH(DataAtracacao),
    YEAR(DataAtracacao)
ORDER BY
    Ano, Mes, Localidade;
