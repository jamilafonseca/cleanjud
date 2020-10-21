#-------------------------------------------------------------------------------
# CNJ Inova -  HACKATHON - Desafio 2 - #Saneamento de Dados
#-------------------------------------------------------------------------------
# SOLUCAO:     CleaNJud - Decisoes mais Eficazes com a Qualidade do DATAJUD
#-------------------------------------------------------------------------------
# Purpose:     MODULO - PARSING JSON
#              Faz a Leitura do arquivo de entrada JSON e Normaliza Dados
#-------------------------------------------------------------------------------
# AUTORES:     (GRUPO 11)
#              Luis Assuncao  - TI  - (lcpassuncao@gmail.com)
#              Jamila Fonseca - ADV - (jamila.fonseca@hotmail.com)
#              Luciana Weiler - UX  - (luciana.weiler.2020@gmail.com)
#-------------------------------------------------------------------------------

# -*- coding: utf-8 -*-
#!/python36/python

# ---------------------------------
#  Import Modules
# ---------------------------------

import pandas as pd   # Data Frame  (pip install pandas)
import re             # funcoes regex
import sys            # Funcoes de interacao com o sist operacional
import os             # Funcoes do sist operacional e arquivos
import os.path        # Funcoes do sist operacional e arquivos
import glob           # Funcoes Diretorio
import datetime       # funcoes de data e hora corrente

#  Inicializacao Variaveis Globais
global count_itens          # numero de arquivos de entrada
global count_valid          # qde de registros validos
global total_valid          # qde de registros validos
global count_processed      # qde de registros processados
global count_files          # qde de arquivos processados
global count_movimentacao   # qde de movimentacoes
global total_movimentacao   # qde de movimentacoes

# --------------------------------------------------------------
# Dicionario LAYOUT hierarquico de ENTRADA - Arquivo formato JSON
# Informacoes basicas do PROCESSO e da MOVIMENTACAO processual
# --------------------------------------------------------------

# KEY (nome_campo) :     ( JSON nivel1, JSON nivel2, JSON nivel3, JSON nivel4, descricao do campo )

reg_json_basico = {
'DATA_INSERCAO'       : ( 'millisInsercao','',               '',        '',  'data e hora quando o arquivo foi indexado no Datajud' ),
'GRAU_JURISDICAO'     : ( 'grau',          '',               '',        '',  'grau de jurisdição em que o processo se encontra' ),
'SIGLA_TRIBUNAL'      : ( 'siglaTribunal', '',               '',        '',  'sigla do Tribunal em que o processo se encontra' ),
'ORGAO_NOME'          : ( 'dadosBasicos',  'orgaoJulgador',  'nomeOrgao', '', 'descrição textual da Unidade Judiciária constante no Módulo de Produtividade' ),
'ORGAO_MUN_IBGE'      : ( 'dadosBasicos',  'orgaoJulgador',  'codigoMunicipioIBGE', '', 'município-sede da unidade judiciária conforme código de municípios do IBGE' ),
'ORGAO_CODIGO'        : ( 'dadosBasicos',  'orgaoJulgador',  'codigoOrgao', '', 'código da Unidade Judiciária constante no Módulo de Produtividade' ),
'ORGAO_INSTANCIA'     : ( 'dadosBasicos',  'orgaoJulgador',  'instancia',   '', 'tipos de instância ' ),
'NUM_PROCESSO'        : ( 'dadosBasicos',  'numero',          '',      '',  'numeração única do processo conforme determinado pela Resolução 65' ),
'CLASSE_PROCESSUAL'   : ( 'dadosBasicos',  'classeProcessual','',      '',  'código da classe processual conforme Resolução 46' ),
'COD_LOCALIDADE'      : ( 'dadosBasicos',  'codigoLocalidade','',      '',  'código identificador da localidade a que pertence ou deve pertencer o processo' ),
'DATA_AJUIZAMENTO'    : ( 'dadosBasicos',  'dataAjuizamento', '',      '',  'data em que o processo foi inicialmente recebido pelo Poder Judiciário' ),
'TIPO_PROCESSO'       : ( 'dadosBasicos',  'procEl',          '',      '',  'campo identifica se o processo tramita em sistema eletrônico ou em papel' ),
'SIST_ELETRONICO'     : ( 'dadosBasicos',  'dscSistema',      '',      '',  'identifica qual o sistema eletrônico que o processo tramita' ),
'VALOR_CAUSA'         : ( 'dadosBasicos',  'valorCausa',      '',      '',  'valor da causa' ),
#'NUM_PROCESSO_OUTROS' : ( 'dadosBasicos',  'outrosnumeros',   '',      '',  'outros números que o processo possa ter recebido durante sua vida' ),
#'RELACAO_INCIDENTAL'  : ( 'dadosBasicos',  'relacaoIncidental','',     '',  'identificar se existe algum elemento incidental que tenha gerado novo processo' ),
#'PRIORIDADE'          : ( 'dadosBasicos',  'prioridade',      '',      '',  'identificação da existência de priopriedades processuais' ),
#'COMPETENCIA'         : ( 'dadosBasicos',  'competencia',     '',      '',  'competência a que pertence o processo' ),
#'NIVEL_SIGILO'        : ( 'dadosBasicos',  'nivelSigilo',     '',      '',  'nível de sigilo a ser aplicado ao processo' ),
#'INTERVENCAO_MP'      : ( 'dadosBasicos',  'intervencaoMP',   '',      '',  'identifica se o processo exige a intervenção do Ministério Público' ),
#'BYTES_PROCESSO'      : ( 'dadosBasicos',  'tamanhoProcesso', '',      '',  'volume em bytes dos documentos existentes no processo judicial' ),
 }

reg_json_assunto = {
'ASSUNTO1_COD_LOCAL'   : ( 0, 'dadosBasicos', 'assunto', 'assuntoLocal',   'codigoAssunto',     'informação relativa ao código numérico utilizado localmente pelo tribunal' ),
'ASSUNTO1_COD_PAI'     : ( 0, 'dadosBasicos', 'assunto', 'assuntoLocal',   'codigoPaiNacional', 'código de assunto nacional de que o assunto local é filho' ),
'ASSUNTO1_DESCRICAO'   : ( 0, 'dadosBasicos', 'assunto', 'assuntoLocal',   'descricao',         'descrição textual do assunto local' ),
'ASSUNTO1_PRINCIPAL'   : ( 0, 'dadosBasicos', 'assunto', 'principal',      '',                  'informa se o assunto referido é o assunto principal do processo.' ),
'ASSUNTO1_COD_NACIONAL': ( 0, 'dadosBasicos', 'assunto', 'codigoNacional', '',                  'código de assunto da tabela nacional unificada decorrente da Resolução 46' ),
'ASSUNTO2_COD_LOCAL'   : ( 1, 'dadosBasicos', 'assunto', 'assuntoLocal',   'codigoAssunto',     'informação relativa ao código numérico utilizado localmente pelo tribunal' ),
'ASSUNTO2_COD_PAI'     : ( 1, 'dadosBasicos', 'assunto', 'assuntoLocal',   'codigoPaiNacional', 'código de assunto nacional de que o assunto local é filho' ),
'ASSUNTO2_DESCRICAO'   : ( 1, 'dadosBasicos', 'assunto', 'assuntoLocal',   'descricao',         'descrição textual do assunto local' ),
'ASSUNTO2_PRINCIPAL'   : ( 1, 'dadosBasicos', 'assunto', 'principal',      '',                  'informa se o assunto referido é o assunto principal do processo.' ),
'ASSUNTO2_COD_NACIONAL': ( 1, 'dadosBasicos', 'assunto', 'codigoNacional', '',                  'código de assunto da tabela nacional unificada decorrente da Resolução 46' ),
'ASSUNTO3_COD_LOCAL'   : ( 2, 'dadosBasicos', 'assunto', 'assuntoLocal',   'codigoAssunto',     'informação relativa ao código numérico utilizado localmente pelo tribunal' ),
'ASSUNTO3_COD_PAI'     : ( 2, 'dadosBasicos', 'assunto', 'assuntoLocal',   'codigoPaiNacional', 'código de assunto nacional de que o assunto local é filho' ),
'ASSUNTO3_DESCRICAO'   : ( 2, 'dadosBasicos', 'assunto', 'assuntoLocal',   'descricao',         'descrição textual do assunto local' ),
'ASSUNTO3_PRINCIPAL'   : ( 2, 'dadosBasicos', 'assunto', 'principal',      '',                  'informa se o assunto referido é o assunto principal do processo.' ),
'ASSUNTO3_COD_NACIONAL': ( 2, 'dadosBasicos', 'assunto', 'codigoNacional', '',                  'código de assunto da tabela nacional unificada decorrente da Resolução 46' ),
}

reg_json_movimento = {
'MOV_DATA'            : ( 'movimento', 'dataHora',                 '',                '',  'momento em que foi realizada a movimentação' ),
'MOV_COD_NACIONAL'    : ( 'movimento', 'movimentoNacional',      'codigoNacional',    '',  'código do movimento previsto na tabela unificada de que trata a Resolução 46' ),
'MOV_COD_PAI'         : ( 'movimento', 'movimentoLocal',         'codigoPaiNacional', '',  'código de movimento nacional de que o movimento local é filho' ),
#'MOV_CODIGO'          : ( 'movimento', 'identificadorMovimento',   '',               '',  'identificador específico para a movimentação realizada' ),
#'MOV_COD_LOCAL'       : ( 'movimento', 'movimentoLocal',        'codigoMovimento',   '',  'código numérico utilizado localmente pelo tribunal' ),
#'MOV_SIGILO'          : ( 'movimento', 'nivelSigilo',              '',   '',              'nível de sigilo a ser aplicado ao processo' ),
#'MOV_RESPONSAVEL'     : ( 'movimento', 'tipoResponsavelMovimento', '',   '',              'identificação do responsável pelo movimento: Servidor=0 Magistrado=1' ),
#'MOV_COMPL'           : ( 'movimento', 'complementoNacional', 'codComplemento',        '', 'código docomplemento do movimentonacional ou do movimento local' ),
#'MOV_COMPL_DESC'      : ( 'movimento', 'complementoNacional', 'descricaoComplemento',  '', 'descrição textualdocomplemento do movimento nacional ou do movimento local' ),
#'MOV_COMPL_TABELA'    : ( 'movimento', 'complementoNacional', 'codComplementoTabelado','', 'código do complemento tabelado do movimento nacional ou do movimento local' ),
#'MOV_ID_DOC'          : ( 'movimento', 'idDocumentoVinculado',      '',           '',          'vinculação de um ou mais documentos à movimentação' ),
#'MOV_ORGAO_NOME'      : ( 'movimento', 'orgaoJulgador',       'dadosBasicos',     'nomeOrgao', 'Unidade Judiciária constante no Módulo de Produtividade Anexo II da Resolução 76' ),
#'MOV_ORGAO_MUN_IBGE'  : ( 'movimento', 'orgaoJulgador',       'dadosBasicos',     'codigoMunicipioIBGE', 'município-sede da unidade judiciária código de municípios do IBGE' ),
#'MOV_ORGAO_CODIGO'    : ( 'movimento', 'orgaoJulgador',       'dadosBasicos',     'codigoOrgao', 'código da Unidade Judiciária constante no Módulo de Produtividade' ),
#'MOV_ORGAO_INSTANCIA' : ( 'movimento', 'orgaoJulgador',       'dadosBasicos',     'instancia',   'tipos de instância ' ),
#'MOV_TIPO_DECISAO'    : ( 'movimento', 'tipoDecisao',         '',                  '',           'decisão monocrática (proferida por um magistrado) ou colegiada; 0 - decisão MONOCRATICA 1 - decisão COLEGIADA' ),
}

# --------------------------------------------------------------
# Dicionario LAYOUT de SAIDA
# Arquivo de entrada JSON desnormalizado (colunar)
# --------------------------------------------------------------

reg_output = {
  'DATA_INSERCAO':        ('', 0),
  'DATA_AJUIZAMENTO':     ('', 0),
  'NUM_PROCESSO':         ('', 0),
  'GRAU_JURISDICAO':      ('', 0),
  'SIGLA_TRIBUNAL':       ('', 0),
  'ORGAO_NOME':           ('', 0),
  'ORGAO_CODIGO':         ('', 0),
  'ORGAO_INSTANCIA':      ('', 0),
  'ORGAO_MUN_IBGE':       ('', 0),
  'COD_LOCALIDADE':       ('', 0),
  'CLASSE_PROCESSUAL':    ('', 0),
  'TIPO_PROCESSO':        ('', 0),
  'SIST_ELETRONICO':      ('', 0),
  'VALOR_CAUSA':          ('', 0),
  'ASSUNTO1_COD_LOCAL':   ('', 0),
  'ASSUNTO1_COD_PAI':     ('', 0),
  'ASSUNTO1_DESCRICAO':   ('', 0),
  'ASSUNTO1_PRINCIPAL':   ('', 0),
  'ASSUNTO1_COD_NACIONAL':('', 0),
  'ASSUNTO2_COD_LOCAL':   ('', 0),
  'ASSUNTO2_COD_PAI':     ('', 0),
  'ASSUNTO2_DESCRICAO':   ('', 0),
  'ASSUNTO2_PRINCIPAL':   ('', 0),
  'ASSUNTO2_COD_NACIONAL':('', 0),
  'ASSUNTO3_COD_LOCAL':   ('', 0),
  'ASSUNTO3_COD_PAI':     ('', 0),
  'ASSUNTO3_DESCRICAO':   ('', 0),
  'ASSUNTO3_PRINCIPAL':   ('', 0),
  'ASSUNTO3_COD_NACIONAL':('', 0),
  'MOV_026_DISTRIB':      ('', 0),
  'MOV_193_SENTENCA':     ('', 0),
  'MOV_848_JULGADO':      ('', 0),
  'MOV_022_BAIXA':        ('', 0),
  'MOV_246_ARQUIVA':      ('', 0),
  'MOV_NAC_TODOS':        ('', 0),
  'MOV_QUANTIDADE':       ('', 0),
 }

# --------------------------------------------------------------
# Funcao de LOG (relatorio de execucao)
# --------------------------------------------------------------

def f_log_trace(step, message):

     now = datetime.datetime.now()

     f_log = open(arq_log, 'a+', encoding="utf-8")

     if (step == 'begin'):
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('  LOG REPORT - ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('  CNJ Inova -  HACKATHON - Desafio 2    ' + '\n')
         f_log.write('  CleaNJud  -  Saneamento do DATAJUD    ' + '\n')
         f_log.write('  Version:     17/10/2020 - v 1.0.0     ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('Start at        : ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n')
         f_log.write('Input  Directory: ' + dir_input + '\n')
         f_log.write('Output Directory: ' + dir_output + '\n')

     if (step == 'close'):
         f_log.write('Arq JSON Processados= ' + str(count_processed) + '\n' )
         f_log.write('Qde Processos       = ' + str(total_valid) + '\n' )
         f_log.write('Qde Movimentacoes   = ' + str(total_movimentacao) + '\n' )
         f_log.write('Completo:  ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n\n')
         campo = ''
         f_log.write('Preenchimento:\n')
         for i in reg_output:
               campo_qde = "{:09d}".format(reg_output[i][1])
               campo_nome = '{:30}'.format(i)
               if  count_valid > 0:
                    campo_perc =  "{:03.2f}".format(100 * (reg_output[i][1] / total_valid))
                    f_log.write( campo_nome + ' : ' + campo_qde + '  perc:(' + campo_perc + '%)  \n')

     if (step == 'error'):
           f_log.write(message + '\n')

     if (step == 'message'):
           f_log.write(message + '\n')

     f_log.close()

# ---------------------------------
#   Inicializa posicao dos blocos
# ---------------------------------

def f_zera_variaveis():

    for key in reg_output:
        reg_output[key] = ('', reg_output[key][1])


# -----------------------------------------------------------------------
#  Leitura da Tabela de Movimentos PRINCIPAIS
#  Base de dados de COD_MOVIMENTO SIMPLIFICADA
#  possui uma lista dos eventos principais (marcos)
# -----------------------------------------------------------------------

def f_movimento_read():

    global dic_movimento
    dic_movimento = {}

    nome_kbnl = 'cleanjud_movimento.csv'
    file_kbnl = os.path.join(os.path.abspath(os.path.dirname(__file__)), nome_kbnl)

    if not os.path.isfile(file_kbnl):
        print("Arquivo <" + nome_kbnl + ">  nao encontrado")
        sys.exit(-1)

    fileObj = open(file_kbnl, 'r', encoding="utf-8")
    content = fileObj.read()
    fileObj.close()

    for line in content.split('\n'):
              c = line.split(";")
              dic_movimento[c[0]] = c[1:]


# --------------------------------------------------------------
# Funcao de PARSE (leitura e formatacao) do arquivo JSON
# Transforma o formato JSON em Colunar (desnormaliza).
# Essa tranformacao visa facilitar as analises por varredura.
# --------------------------------------------------------------

def f_parse_json( json_file ):

    reg_processados = 0
    lista_todos_movimentos = ''
    count_movimentacao = 0

    df = pd.read_json (json_file)

    for jrow in range(len(df)):

        # Captura as informacoes DadosBasicos do JSON
        for key in reg_json_basico:

                    nivel1 = reg_json_basico[key][0]
                    nivel2 = reg_json_basico[key][1]
                    nivel3 = reg_json_basico[key][2]
                    nivel4 = reg_json_basico[key][3]

                    try:
                        if  (nivel2 == ''):
                               conteudo =  df[nivel1][jrow]
                        elif (nivel3 == ''):
                               conteudo =  df[nivel1][jrow][nivel2]
                        elif (nivel4 == ''):
                               conteudo =  df[nivel1][jrow][nivel2][nivel3]
                        else:
                               conteudo =  df[nivel1][jrow][nivel2][nivel3][nivel4]
                    except:
                               conteudo = ''

                    f_registra_mem(key, str(conteudo))


        # Captura as informacoes de ASSUNTOS do JSON
        # coloca as ocorrencias de Assuntos em colunas
        for key in reg_json_assunto:

                    iassunto = reg_json_assunto[key][0]
                    nivel1 = reg_json_assunto[key][1]
                    nivel2 = reg_json_assunto[key][2]
                    nivel3 = reg_json_assunto[key][3]
                    nivel4 = reg_json_assunto[key][4]

                    try:
                        if  (nivel4 == ''):
                               conteudo =  df[nivel1][jrow][nivel2][iassunto][nivel3]
                        else:
                               conteudo =  df[nivel1][jrow][nivel2][iassunto][nivel3][nivel4]
                    except:
                               conteudo = ''

                    f_registra_mem(key, str(conteudo))


        # Captura as informacoes das MOVIMENTACOES do JSON
        # coloca as ocorrencias de Movimentos em colunas

        try:
           qde_movimentos = len(df['movimento'][jrow])
        except:
           qde_movimentos = 0

        count_movimentacao += qde_movimentos

        for j in range(qde_movimentos):

                 v_movimento = ''
                 v_dataHora = ''
                 v_codigoNacional = ''
                 v_codigoPaiNacional = ''

                 try:
                    v_dataHora = str(df['movimento'][jrow][j]['dataHora'])
                 except:
                    v_dataHora = ''

                 try:
                    v_codigoNacional = str(df['movimento'][jrow][j]['movimentoNacional']['codigoNacional'])
                 except:
                    v_codigoNacional = ''

                 try:
                    v_codigoPaiNacional = str( df['movimento'][jrow][j]['movimentoLocal']['codigoPaiNacional'])
                 except:
                    v_codigoPaiNacional = ''

                 if (v_codigoNacional == ''):
                        v_movimento = v_codigoPaiNacional
                 else:
                        v_movimento = v_codigoNacional

                 if (("|" + v_movimento + "|") not in ("|" + lista_todos_movimentos)):
                     lista_todos_movimentos +=  v_movimento + "|"

                 if (v_movimento in dic_movimento ):
                       mov_principal = dic_movimento[v_movimento][0]
                       f_registra_mem(mov_principal, str(v_dataHora))

        f_registra_mem('MOV_NAC_TODOS', lista_todos_movimentos)
        f_registra_mem('MOV_QUANTIDADE', str(qde_movimentos))
        lista_todos_movimentos = ''

        # Grava os registros no Arquivo CSV de saida
        for key in reg_output:
            f_out.write(reg_output[key][0] + ";")
        reg_processados = reg_processados + 1
        f_out.write("\n")
        f_zera_variaveis()

    return reg_processados, count_movimentacao

# ---------------------------------
#   Registra na Memoria campo de saida
# ---------------------------------

def f_registra_mem(key, conteudo):

    if (len(conteudo) > 0):
         inc = 1
    else:
         inc = 0

    conteudo = conteudo.translate(bytes.maketrans(b";", b" "))   # retira delimitadores
    conteudo = re.sub('"', ' ', conteudo)  # retira aspas duplas
    conteudo = re.sub("'", ' ', conteudo)  # retira aspas simples
    conteudo = re.sub('\n', ' ', conteudo) # retira quebra de linha
    conteudo = re.sub('\s+',' ', conteudo) # retira espacos duplos desnecessarios

    reg_output[key] = (conteudo, reg_output[key][1] + inc )


# --------------------------------------------------------------
# MAIN - Corpo Principal
# --------------------------------------------------------------

# Constantes Iniciais - Parametros   - CONFIGURAR NA INSTALACAO !!!
# ------------------------------------------------------------
dir_input  = 'C:\\bases_CNJ'        # diretorio RAIZ de entrada (pra pegar todos os arquivos  JSON)
dir_output = 'D:\\CNJ\output\\'     # diretorio de saida
dir_log    = 'D:\\CNJ\LOG\\'        # diretorio de LOG
# ------------------------------------------------------------


#  Inicializacao Variaveis Globais
count_itens = 0         # numero de arquivos de entrada
count_valid = 0         # qde de registros validos
total_valid = 0         # qde de registros validos
count_processed = 0     # qde de registros processados
count_files = 0         # qde de arquivos processados
count_movimentacao = 0  # qde de movimentacoes dos processos
total_movimentacao = 0  # qde de movimentacoes dos processos

# Faz a abertura dos Arquivos de Saida e LOG
now = datetime.datetime.now()
data_atualizacao = now.strftime("%Y-%m-%d")

# Arquivos de LOG e Arquivo de Saida
arq_log       = dir_log    + 'log_cleanjud_1_parse_' + now.strftime("%Y_%m_%d__%H%M%S") + '.txt'
arq_out       = dir_output + 'cnj_datajud.txt'

f_log = open(arq_log, 'w', encoding="utf-8") # Abre Arquivo de LOG
f_log.close()
f_out = open(arq_out, 'w', encoding="utf-8") # Abre Arquivo de saida

# Carrega Parametros dos COD MOVIMENTOS principais
# pra memoria
f_movimento_read()

# Conta a qde arquivos a serem processados no diretorio
for root, dirs, files in os.walk(dir_input):
    for file in files:
        if file.endswith(".json"):
             count_files = count_files + 1


# Inicia o registro do LOG - Relatorio execucao
f_log_trace("begin","")
print('Inicio execucao : '  + now.strftime("%Y-%m-%d %H:%M:%S") )
print('Diretorio Entrada: ' + dir_input )
print('Diretorio Saida:   ' + dir_output)
print('Qde Arquivos:      ' + str(count_files))

# Print File Header - nome dos campos
for key in reg_output:
        f_out.write(key + ";")
f_out.write("\n")

# Para cada um arquivo JSON dos diretorios e subdiretorios
for root, dirs, files in os.walk(dir_input):
    for file in files:
        if file.endswith(".json"):
             f_zera_variaveis()
             arq_json_entrada =   os.path.join(root, file)
             count_valid, count_movimentacao = f_parse_json(arq_json_entrada)
             total_valid += count_valid
             total_movimentacao +=  count_movimentacao
             count_processed += 1
             if (count_processed % 10) == 0:
                 print("#", end = '')

# Finaliza o registro do LOG
f_log_trace("close", "")

# Fecha do Arquivo de Saida
f_out.close()

print("DONE!")