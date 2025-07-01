def extrair_base_x():
    return

def extrair_base_y():
    return

def comparar_volumetria(df_original, df_aws):
    volumetria_original = len(df_original)
    volumetria_aws = len(df_aws)
    diferenca = volumetria_original - volumetria_aws
    return {
        'volumetria_original': volumetria_original,
        'volumetria_aws': volumetria_aws,
        'diferenca': diferenca
    }

def contratos_ausentes(df_original, df_aws, coluna_contrato):
    contratos_originais = set(df_original[coluna_contrato].unique())
    contratos_aws = set(df_aws[coluna_contrato].unique())
    ausentes = contratos_originais - contratos_aws
    df_ausentes = pd.DataFrame(list(ausentes), columns=[coluna_contrato])
    numero_ausentes = len(ausentes)
    amostra = df_ausentes.sample(min(5, numero_ausentes)) if numero_ausentes > 0 else pd.DataFrame()
    df_ausentes.to_csv('contratos_ausentes.csv', index=False)
    return df_ausentes, numero_ausentes, amostra


def comparar_volumetria_filtrada(df_original, df_aws, coluna_contrato):
    contratos_comuns = set(df_original[coluna_contrato].unique()) & set(df_aws[coluna_contrato].unique())
    df_original_filtrado = df_original[df_original[coluna_contrato].isin(contratos_comuns)]
    df_aws_filtrado = df_aws[df_aws[coluna_contrato].isin(contratos_comuns)]
    volumetria_original = len(df_original_filtrado)
    volumetria_aws = len(df_aws_filtrado)
    diferenca = volumetria_original - volumetria_aws
    return {
        'volumetria_original_filtrada': volumetria_original,
        'volumetria_aws_filtrada': volumetria_aws,
        'diferenca_filtrada': diferenca
    }


def comparar_ultimas_particoes(df_original, df_aws, coluna_contrato, coluna_particao):
    contratos_comuns = set(df_original[coluna_contrato].unique()) & set(df_aws[coluna_contrato].unique())
    df_original_ultimas = df_original.loc[df_original.groupby(coluna_contrato)[coluna_particao].idxmax()]
    df_aws_ultimas = df_aws.loc[df_aws.groupby(coluna_contrato)[coluna_particao].idxmax()]
    volumetria_original = len(df_original_ultimas)
    volumetria_aws = len(df_aws_ultimas)
    return volumetria_original == volumetria_aws


def comparar_colunas_por_contrato(df_original, df_aws, coluna_contrato, colunas_comparar):
    contratos_comuns = set(df_original[coluna_contrato].unique()) & set(df_aws[coluna_contrato].unique())
    diferencas_por_coluna = {col: 0 for col in colunas_comparar}
    for contrato in contratos_comuns:
        df_orig_contrato = df_original[df_original[coluna_contrato] == contrato]
        df_aws_contrato = df_aws[df_aws[coluna_contrato] == contrato]
        for col in colunas_comparar:
            valor_orig = df_orig_contrato[col].iloc[-1] if not df_orig_contrato.empty else None
            valor_aws = df_aws_contrato[col].iloc[-1] if not df_aws_contrato.empty else None
            if valor_orig != valor_aws:
                diferencas_por_coluna[col] += 1
    return diferencas_por_coluna

def analisar_valores_nulos(df_original, df_aws, colunas_analisar=None):
    """
    Compara a proporção de valores nulos entre as bases original e AWS
    """
    if colunas_analisar is None:
        colunas_analisar = df_original.columns.intersection(df_aws.columns)
    
    analise_nulos = {}
    
    for coluna in colunas_analisar:
        nulos_original = df_original[coluna].isnull().sum()
        total_original = len(df_original)
        prop_nulos_original = (nulos_original / total_original) * 100
        
        nulos_aws = df_aws[coluna].isnull().sum()
        total_aws = len(df_aws)
        prop_nulos_aws = (nulos_aws / total_aws) * 100
        
        diferenca_prop = abs(prop_nulos_original - prop_nulos_aws)
        
        analise_nulos[coluna] = {
            'nulos_original': nulos_original,
            'prop_nulos_original': round(prop_nulos_original, 2),
            'nulos_aws': nulos_aws,
            'prop_nulos_aws': round(prop_nulos_aws, 2),
            'diferenca_proporcao': round(diferenca_prop, 2),
            'alerta': diferenca_prop > 5.0  # Alerta se diferença > 5%
        }
    
    return analise_nulos

def gerar_relatorio_nulos(analise_nulos):
    """
    Gera relatório formatado da análise de valores nulos
    """
    colunas_com_alerta = [col for col, dados in analise_nulos.items() if dados['alerta']]
    
    print(f"📊 ANÁLISE DE VALORES NULOS")
    print(f"{'='*50}")
    print(f"Total de colunas analisadas: {len(analise_nulos)}")
    print(f"Colunas com diferenças significativas (>5%): {len(colunas_com_alerta)}")
    
    if colunas_com_alerta:
        print(f"\n⚠️  COLUNAS COM ALERTAS:")
        for coluna in colunas_com_alerta:
            dados = analise_nulos[coluna]
            print(f"  • {coluna}: {dados['prop_nulos_original']}% → {dados['prop_nulos_aws']}% "
                  f"(diferença: {dados['diferenca_proporcao']}%)")
    
    return colunas_com_alerta


def detectar_duplicatas(df_original, df_aws, coluna_contrato, colunas_chave=None):
    """
    Detecta duplicatas nas bases e compara os padrões entre original e AWS
    """
    if colunas_chave is None:
        colunas_chave = [coluna_contrato]
    
    # Análise de duplicatas na base original
    duplicatas_original = df_original.duplicated(subset=colunas_chave, keep=False)
    total_duplicatas_original = duplicatas_original.sum()
    contratos_duplicados_original = df_original[duplicatas_original][coluna_contrato].nunique()
    
    # Análise de duplicatas na base AWS
    duplicatas_aws = df_aws.duplicated(subset=colunas_chave, keep=False)
    total_duplicatas_aws = duplicatas_aws.sum()
    contratos_duplicados_aws = df_aws[duplicatas_aws][coluna_contrato].nunique()
    
    # Identificar contratos que se tornaram duplicados na AWS
    contratos_unicos_original = set(df_original[coluna_contrato].unique())
    contratos_duplicados_aws_set = set(df_aws[duplicatas_aws][coluna_contrato].unique())
    novos_duplicados = contratos_duplicados_aws_set - set(df_original[duplicatas_original][coluna_contrato].unique())
    
    resultado = {
        'duplicatas_original': {
            'total_registros': total_duplicatas_original,
            'contratos_afetados': contratos_duplicados_original,
            'proporcao': round((total_duplicatas_original / len(df_original)) * 100, 2)
        },
        'duplicatas_aws': {
            'total_registros': total_duplicatas_aws,
            'contratos_afetados': contratos_duplicados_aws,
            'proporcao': round((total_duplicatas_aws / len(df_aws)) * 100, 2)
        },
        'novos_duplicados': {
            'contratos': list(novos_duplicados),
            'quantidade': len(novos_duplicados)
        }
    }
    
    return resultado

def analisar_duplicatas_por_contrato(df_aws, coluna_contrato, coluna_particao):
    """
    Analisa especificamente os contratos que aparecem em múltiplas partições na AWS
    """
    contagem_por_contrato = df_aws.groupby(coluna_contrato).size()
    contratos_multiplas_particoes = contagem_por_contrato[contagem_por_contrato > 1]
    
    # Estatísticas detalhadas
    estatisticas = {
        'total_contratos_duplicados': len(contratos_multiplas_particoes),
        'max_ocorrencias': contratos_multiplas_particoes.max() if len(contratos_multiplas_particoes) > 0 else 0,
        'media_ocorrencias': round(contratos_multiplas_particoes.mean(), 2) if len(contratos_multiplas_particoes) > 0 else 0,
        'distribuicao_ocorrencias': contratos_multiplas_particoes.value_counts().sort_index().to_dict()
    }
    
    # Amostra de contratos com múltiplas partições
    amostra_contratos = contratos_multiplas_particoes.head(5).to_dict()
    
    return estatisticas, amostra_contratos

def gerar_relatorio_duplicatas(resultado_duplicatas, estatisticas_particoes, amostra_contratos):
    """
    Gera relatório formatado da análise de duplicatas
    """
    print(f"🔍 ANÁLISE DE DUPLICATAS")
    print(f"{'='*50}")
    
    # Comparação geral
    print(f"📈 COMPARAÇÃO GERAL:")
    print(f"  Original: {resultado_duplicatas['duplicatas_original']['total_registros']} registros duplicados "
          f"({resultado_duplicatas['duplicatas_original']['proporcao']}%)")
    print(f"  AWS: {resultado_duplicatas['duplicatas_aws']['total_registros']} registros duplicados "
          f"({resultado_duplicatas['duplicatas_aws']['proporcao']}%)")
    
    # Novos duplicados
    if resultado_duplicatas['novos_duplicados']['quantidade'] > 0:
        print(f"\n⚠️  NOVOS CONTRATOS DUPLICADOS NA AWS:")
        print(f"  Quantidade: {resultado_duplicatas['novos_duplicados']['quantidade']}")
        print(f"  Amostra: {resultado_duplicatas['novos_duplicados']['contratos'][:5]}")
    
    # Análise de múltiplas partições
    print(f"\n📊 CONTRATOS EM MÚLTIPLAS PARTIÇÕES (AWS):")
    print(f"  Total de contratos: {estatisticas_particoes['total_contratos_duplicados']}")
    print(f"  Máximo de ocorrências: {estatisticas_particoes['max_ocorrencias']}")
    print(f"  Média de ocorrências: {estatisticas_particoes['media_ocorrencias']}")
    
    if amostra_contratos:
        print(f"\n📋 AMOSTRA DE CONTRATOS COM MÚLTIPLAS PARTIÇÕES:")
        for contrato, ocorrencias in amostra_contratos.items():
            print(f"  • Contrato {contrato}: {ocorrencias} ocorrências")


def executar_analises_qualidade(df_original, df_aws, coluna_contrato, coluna_particao=None, colunas_analisar=None):
    """
    Executa análises de valores nulos e duplicatas de forma integrada
    """
    print("🚀 INICIANDO ANÁLISES DE QUALIDADE DE DADOS")
    print("="*60)
    
    # Análise de valores nulos
    analise_nulos = analisar_valores_nulos(df_original, df_aws, colunas_analisar)
    colunas_alerta = gerar_relatorio_nulos(analise_nulos)
    
    print("\n")
    
    # Análise de duplicatas
    resultado_duplicatas = detectar_duplicatas(df_original, df_aws, coluna_contrato)
    
    if coluna_particao:
        estatisticas_particoes, amostra_contratos = analisar_duplicatas_por_contrato(df_aws, coluna_contrato, coluna_particao)
    else:
        estatisticas_particoes = {'total_contratos_duplicados': 0, 'max_ocorrencias': 0, 'media_ocorrencias': 0}
        amostra_contratos = {}
    
    gerar_relatorio_duplicatas(resultado_duplicatas, estatisticas_particoes, amostra_contratos)
    
    return {
        'analise_nulos': analise_nulos,
        'colunas_com_alerta_nulos': colunas_alerta,
        'analise_duplicatas': resultado_duplicatas,
        'estatisticas_particoes': estatisticas_particoes
    }
