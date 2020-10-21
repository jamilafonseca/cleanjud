#-------------------------------------------------------------------------------
# CNJ Inova -  HACKATHON - Desafio 2 - #Saneamento de Dados
#-------------------------------------------------------------------------------
# SOLUCAO:     CleaNJud - Decisoes mais Eficazes com a Qualidade do DATAJUD
#-------------------------------------------------------------------------------
# Purpose:     MODULO 4 - Machine Learning e Modelo de Outliers
#              - Faz agrupamento dos procesoss por um Grupo de Variaveis
#              - Faz a contagem e o percentual de distribuicao
#              - Ordena pelo Percentual e calcula o percentual acumulado
#              - Gera base de conhecimento com SIGMA 2 (95,44%)
#              - Aplica a base de conhecimento pra identificar os Outliers
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

import pandas as pd      # Data Frame  (pip install pandas)
import re                # funcoes regex
import sys               # Funcoes de interacao com o sist operacional
import os                # Funcoes do sist operacional e arquivos
import os.path           # Funcoes do sist operacional e arquivos
import glob              # Funcoes Diretorio
import datetime          # funcoes de data e hora corrente
import time              # funcoes de data e hora corrente
import operator          # usado na funcao de ordenacao (sort)

#  Inicializacao Variaveis Globais
global count_itens       # numero de arquivos de entrada
global count_valid       # qde de registros validos
global count_processed   # qde de registros processados
global count_files       # qde de arquivos processados


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
         f_log.write('  MODULO 4:    MACHINE LEARNING  ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('  CNJ Inova -  HACKATHON - Desafio 2    ' + '\n')
         f_log.write('  CleaNJud  -  Saneamento do DATAJUD    ' + '\n')
         f_log.write('  Version:     18/10/2020 - v 1.0.0     ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('Start at        : ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n')
         f_log.write('Input  Directory: ' + dir_input + '\n')
         f_log.write('Output Directory: ' + dir_output + '\n')

     if (step == 'close'):
         f_log.write('Registros Processados       = ' + str(count_processed) + '\n\n' )
         f_log.write('Qde Anomalias Identificadas = ' + str(count_anomalias) + '\n\n' )

         f_log.write('Completo:  ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n\n')

     if (step == 'error'):
           f_log.write(message + '\n')

     if (step == 'message'):
           f_log.write(now.strftime("%H:%M:%S") + '   ' + message + '\n')

     f_log.close()


# --------------------------------------------------------------
# Funcao de Calculo do Tipo de Justica a partir do nome do Tribunal
# --------------------------------------------------------------

def f_calc_tipo_justica(tribunal):

    tipo_justica = ''
    tribunal = tribunal.upper()
    tribunal = tribunal.strip()


    if (tribunal[:3] == 'TRE'):
         tipo_justica = 'ELEITORAL'

    if (tribunal[:3] == 'TRF'):
         tipo_justica = 'FEDERAL'

    if (tribunal[:3] == 'TRT'):
         tipo_justica = 'TRABALHO'

    if (tribunal[:2] == 'TJ'):
         tipo_justica = 'ESTADUAL'

    if (len(tribunal) == 5 and tribunal[:3] == 'TJM'):
         tipo_justica = 'MILITAR ESTATUAL'

    if (tipo_justica == ''):
           tipo_justica = tribunal

    return tipo_justica


# ---------------------------------
#  Leitura do Arquivo de Entrada
# ---------------------------------

def f_leitura_entrada(arq_in):

    # Variaveis globais
    # Data Frame de dos dados de entrada
    global data_in
    global header_in
    global reg_input

    f_in = open(arq_in, 'r', encoding="utf-8")
    content = f_in.read()
    f_in.close()

    #inicializa as variaveis
    i = 0
    data_in = []
    header_in = []
    reg_input = {}

    # Varredura do Arquivo de Entrada
    for line in content.split('\n'):
        i = i + 1

        if i == 1:
             header_in.append(line.split(";"))
             for j in range(len(header_in[0])):
                   key = str(header_in[0][j])
                   if key != '':
                         # Captura nome e ordem das coluna HEADER
                         reg_input[key] = j
        else:
                   data_in.append( line.split(";"))

# ----------------------------------------------
#  Leitura da Tabela de Assuntos Hierarquizada
# ----------------------------------------------

def f_carrega_tabela_assuntos():

    global dic_hierarquia_assuntos
    dic_hierarquia_assuntos = {}

    nome_kbnl = 'cleanjud_hierarquia_assunto.csv'
    file_kbnl = os.path.join(os.path.abspath(os.path.dirname(__file__)), nome_kbnl)

    if not os.path.isfile(file_kbnl):
        print("Arquivo <" + nome_kbnl + ">  nao encontrado")
        sys.exit(-1)

    fileObj = open(file_kbnl, 'r', encoding="utf-8")
    content = fileObj.read()
    fileObj.close()

    for line in content.split('\n'):
              c = line.split(";")
              if (len(c) > 3):
                      dic_hierarquia_assuntos[c[0]] = c[1:]



# -------------------------------------------------
#  Converte para o NIVEL especifico da Huerarquia
#  da Tabela de Assuntos  (sobe pro Nivel PAI)
# -------------------------------------------------

def f_calcula_nivel_hierarquia_assunto(cod_assunto, nivel_desejado):


      #Calcula qual a coluna utilizar conforme o Nivel desejado
      coluna = (nivel_desejado * 2) - 2

      if (cod_assunto in dic_hierarquia_assuntos):
            retorno = dic_hierarquia_assuntos[cod_assunto][coluna]
      else:
      # Caso nao encontre o Nivel Pai, mantem o mesmo codigo de Assunto
            retorno = cod_assunto

      if (retorno == ''):
            retorno = cod_assunto

      return retorno


# --------------------------------------------------
#   Agrupa Informacoes por um Grupo de Variaveis
#   e faz a contagem das ocorrencias
# --------------------------------------------------

def f_agrupa_informacoes( indice_entrada):


    i = indice_entrada


    # Seleciona Variaveis de Agrupamento
    v_justica = f_calc_tipo_justica(data_in[i][reg_input['SIGLA_TRIBUNAL']] )
    v_grau = str(data_in[i][reg_input['GRAU_JURISDICAO']] )
    v_grau = v_grau.upper()
    v_classe = data_in[i][reg_input['CLASSE_PROCESSUAL']]
    v_assunto =    data_in[i][reg_input['ASSUNTO1_COD_NACIONAL']]
    if (v_assunto == ''):
        v_assunto =    data_in[i][reg_input['ASSUNTO1_COD_PAI']]
        if (v_assunto == ''):
            v_assunto =    data_in[i][reg_input['ASSUNTO2_COD_NACIONAL']]
            if (v_assunto == ''):
                  v_assunto =    data_in[i][reg_input['ASSUNTO2_COD_PAI']]

    v_assunto = f_calcula_nivel_hierarquia_assunto(v_assunto ,2)


    if (v_assunto != '' and v_classe != ''):

        if (v_justica, v_grau, v_classe, v_assunto) in ML:
            ML[v_justica, v_grau, v_classe, v_assunto] = (ML[v_justica, v_grau, v_classe, v_assunto][0]+1,0,0,0)
        else:
            ML[v_justica, v_grau, v_classe, v_assunto] = (1,0,0,0)


# --------------------------------------------------
#   Soma as quantidades totais pelo nivel PAI:  Justica+Grau
# --------------------------------------------------

def f_calcula_qde_total_hierarquia_pai():

    global ML_agregado
    ML_agregado = {}

    for  v_justica, v_grau, v_classe, v_assunto in ML:

           qde_processos = ML[v_justica, v_grau, v_classe, v_assunto][0]

           if (v_justica, v_grau) in ML_agregado:
              ML_agregado[v_justica, v_grau] = ML_agregado[v_justica, v_grau] + qde_processos
           else:
              ML_agregado[v_justica, v_grau] = qde_processos


# --------------------------------------------------
#   Calcula o Percentual em relacao ao nivel PAI
# --------------------------------------------------

def f_calcula_percentuais():

    qde_pai = 0
    qde_filho = 0
    percentual = 0

    for  v_justica, v_grau, v_classe, v_assunto in ML:

                 qde_pai =   ML_agregado[v_justica, v_grau]
                 qde_filho = ML[v_justica, v_grau, v_classe, v_assunto][0]

                 if (qde_pai == 0):
                    percentual = 0
                 else:
                    percentual = (qde_filho/qde_pai)*100

                 ML[v_justica, v_grau, v_classe, v_assunto] = (qde_filho, qde_pai, percentual, 0)
                 linha = [ v_justica, v_grau, v_classe, v_assunto, percentual, 0]
                 ML_dataframe.append( linha )

# --------------------------------------------------
#   Calcula o Percentual Acumulado
# --------------------------------------------------

def f_calcula_percentual_acumulado():

    perc_acumulado = 0

    for  i in range(len(ML_dataframe)):

       if (i == 1):
            perc_acumulado = ML_dataframe[i][4]
            ML_dataframe[i][5] = perc_acumulado
       else:
            if (ML_dataframe[i][0] == ML_dataframe[i-1][0] and ML_dataframe[i][1] == ML_dataframe[i-1][1]  ):
                perc_acumulado += ML_dataframe[i][4]
                ML_dataframe[i][5] = perc_acumulado
            else:
                perc_acumulado = ML_dataframe[i][4]
                ML_dataframe[i][5] = perc_acumulado

# --------------------------------------------------
#   Gera a base de Conhecimento do Machine Learning
#   (Aprendizado de Maquina NAO Supervisionado)
#   com as informacoes do Percentil superior a 4,56%
#   para utilizar o conjunto de dados do 95,44%
#   equivalente a SIGMA2 da Distribuicao Normal.
# --------------------------------------------------

def f_machine_learning_write():

   # Write File - Grava no Arquivo CSV o resultado da Analise de Distribuicao
   # Gera uma Base de Conhecimento (Aprendizado não supervisionado)

   for i in range(len(ML_dataframe)) :

       # 2 SIGMA - (2nd desvio padrao)  --> 4.56%
       if ( ML_dataframe[i][5] > 4.56):
          f_ml.write(ML_dataframe[i][0] +';'+ ML_dataframe[i][1] +';'+ ML_dataframe[i][2] +';'+ ML_dataframe[i][3] +';'+ str(ML_dataframe[i][4])+';'+ str(ML_dataframe[i][5])+';' + '\n')

   f_ml.close()


# -----------------------------------------------------------------------
#  Leitura da Base de Conhecimento gerada pelo Machine Learning
# -----------------------------------------------------------------------

def f_machine_learning_read():

    global dic_machine_learning
    dic_machine_learning = {}

    #nome_kbnl =  'cleanjud_machine_learning.csv'
    #file_kbnl = os.path.join(os.path.abspath(os.path.dirname(__file__)), nome_kbnl)
    file_kbnl =  arq_ml


    if not os.path.isfile(file_kbnl):
        print("Arquivo <" + nome_kbnl + ">  nao encontrado")
        sys.exit(-1)

    fileObj = open(file_kbnl, 'r', encoding="utf-8")
    content = fileObj.read()
    fileObj.close()

    for line in content.split('\n'):
              c = line.split(";")
              if (len(c) > 3):
                      dic_machine_learning[c[0],c[1],c[2],c[3]] = (c[4],c[5])

# -----------------------------------------------------------------------
#  Gera Arquivo com as possiveis Anomalias (Outliers)
# -----------------------------------------------------------------------

def f_identifica_outliers():

    contador = 0

    for i in range(len(data_in)):

       if (len(data_in[i]) > 5):


           v_justica = f_calc_tipo_justica(data_in[i][reg_input['SIGLA_TRIBUNAL']] )
           v_grau = str(data_in[i][reg_input['GRAU_JURISDICAO']] )
           v_grau = v_grau.upper()
           v_classe = data_in[i][reg_input['CLASSE_PROCESSUAL']]
           v_assunto =    data_in[i][reg_input['ASSUNTO1_COD_NACIONAL']]
           if (v_assunto == ''):
               v_assunto =    data_in[i][reg_input['ASSUNTO1_COD_PAI']]
               if (v_assunto == ''):
                   v_assunto =    data_in[i][reg_input['ASSUNTO2_COD_NACIONAL']]
                   if (v_assunto == ''):
                       v_assunto =    data_in[i][reg_input['ASSUNTO2_COD_PAI']]

           v_assunto = f_calcula_nivel_hierarquia_assunto(v_assunto ,2)

           if (v_assunto != '' and v_classe != ''):
                if ( (v_justica, v_grau, v_classe, v_assunto) not in dic_machine_learning) :

                   for col in range(len(data_in[i])):
                            f_out.write(data_in[i][col] + ";")
                   f_out.write('\n');
                   contador += 1

    return contador

# --------------------------------------------------------------
# MAIN - Corpo Principal
# --------------------------------------------------------------

# Constantes Iniciais - Parametros   - CONFIGURAR NA INSTALACAO !!!
# ------------------------------------------------------------
dir_input  = 'D:\\CNJ\output\\'        # diretorio de entrada
dir_output = 'D:\\CNJ\output\\'     # diretorio de saida
dir_log    = 'D:\\CNJ\LOG\\'        # diretorio de LOG
# ------------------------------------------------------------


#  Inicializacao Variaveis Globais
global ML
global ML_dataframe
global count_anomalias
count_itens = 0         # numero de arquivos de entrada
count_valid = 0         # qde de registros validos
count_processed = 0     # qde de registros processados
count_files = 0         # qde de arquivos processados
count_anomalias = 0     # qde anomalias

# Faz a abertura dos Arquivos de Saida e LOG
now = datetime.datetime.now()
data_atualizacao = now.strftime("%Y-%m-%d")

# Arquivos de LOG e Arquivo de Saida
arq_log  = dir_log    + 'log_cleanjud_4_machine_learning_' + now.strftime("%Y_%m_%d__%H%M%S") + '.txt'
arq_in   = dir_output + 'cnj_datajud.txt'
arq_out  = dir_output + 'cleanjud_anomalias.txt'
arq_ml   = dir_output + 'cleanjud_machine_learning.txt'

f_log = open(arq_log, 'w', encoding="utf-8") # Abre Arquivo de LOG
f_log.close()
f_out = open(arq_out, 'w', encoding="utf-8") # Abre Arquivo de saida
f_ml =  open(arq_ml,  'w', encoding="utf-8") # Abre Arquivo de saida

# Inicia o registro do LOG - Relatorio execucao
f_log_trace("begin","")
print('Inicio execucao :      '  + now.strftime("%Y-%m-%d %H:%M:%S") )
print('Arq de Entrada:        ' + arq_in )
print('Arq Machine Learning:  ' + arq_ml)
print('Arq Saida Outliers:    ' + arq_out)

# Faz a leitura pra memoria das Tabelas Auxiliares
print('... carregando parametros...')
f_log_trace('message', '... carregando parametros...')
f_carrega_tabela_assuntos()

# Faz a leitura do Arquivo de Entrada
# Arquivo resultando da consolidacao dos Arquivos JSON CNJ
print('... leitura do arquivo de entrada...')
f_log_trace('message', '... leitura do arquivo de entrada...')
f_leitura_entrada(arq_in)

count_processed = 0

ML = {}
ML_dataframe = []

# Faz a varredura no Arquivo de Entrada
print('... inicio Modelo Aprendizado ...')
f_log_trace('message', '... inicio Modelo Aprendizado ...')
print('... Agrupa Variaveis de Comparacao ...')

for idx_entrada in range(len(data_in)):
   if (len(data_in[idx_entrada]) > 5):
      f_agrupa_informacoes(idx_entrada)
      count_processed += 1
   if (count_processed % 150000) == 0:
       print("#", end = '')

print('\n' + '... Calculo dos Percentuais Distribuicao ...')
f_log_trace('message', '... Calculo dos Percentuais Distribuicao ...')
f_calcula_qde_total_hierarquia_pai()
f_calcula_percentuais()

print('... Ordenacao dos Percentis ...')
ML_dataframe = sorted(ML_dataframe, key = operator.itemgetter(0, 1, 4))

print('... Calculo do Percentual Acumulado ...')
f_calcula_percentual_acumulado()

print('... gravando a base de conhecimento do Machine Learning ...')
f_log_trace('message', '... gravando a base de conhecimento do Machine Learning ...')
f_machine_learning_write()

print('... Carrega a base de conhecimento pra memoria ...')
f_log_trace('message', '... Carrega a base de conhecimento pra memoria ...')
f_machine_learning_read()

print('... Grava Arquivo de OUTLIERS (provaveis anomalias) ...')
f_log_trace('message', '... Grava Arquivo de OUTLIERS (provaveis anomalias) ...')
count_anomalias = f_identifica_outliers()

# Finaliza o registro do LOG
f_log_trace("close", "")

# Fecha do Arquivo de Saida
f_out.close()

print("DONE!")
