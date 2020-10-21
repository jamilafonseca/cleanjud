#-------------------------------------------------------------------------------
# CNJ Inova -  HACKATHON - Desafio 2 - #Saneamento de Dados
#-------------------------------------------------------------------------------
# SOLUCAO:     CleaNJud - Decisoes mais Eficazes com a Qualidade do DATAJUD
#-------------------------------------------------------------------------------
# Purpose:     MODULO 5 - Correcao e Enriquecimento de Dados
#              - Corrige Data Ajuizamento a partir da Data Distribuicao
#              - Corrige COD_ORGAO, COD_IBGE a partir do NUM_PROCESSO_CNJ7
#              - Enriquece Informacoes a partir do COD_ORGAO
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

#  Inicializacao Variaveis Globais
global count_itens       # numero de arquivos de entrada
global count_valid       # qde de registros validos
global count_processed   # qde de registros processados
global count_files       # qde de arquivos processados


# --------------------------------------------------------------
# Dicionario LAYOUT de SAIDA
# Arquivo de entrada JSON desnormalizado (colunar)
# --------------------------------------------------------------
                                                                  # CORRECOES UTILIZADAS
reg_output = {
  'DATA_AJUIZAMENTO':        ('01-DATA_AJUIZAMENTO'       , 0),   #  <-- DATA MOV_DISTRIBUCAO
  'NUM_PROCESSO':            ('02-NUM_PROCESSO'           , 0),
  'GRAU_JURISDICAO':         ('03-GRAU_JURISDICAO'        , 0),
  'SIGLA_TRIBUNAL':          ('04-SIGLA_TRIBUNAL'         , 0),   #  <-- TABELA SERVENTIA
  'ORGAO_NOME':              ('05-ORGAO_NOME'             , 0),   #  <-- TABELA SERVENTIA
  'ORGAO_CODIGO':            ('06-ORGAO_CODIGO'           , 0),   #  <-- DE-PARA CNJ7
  'ORGAO_MUN_IBGE':          ('07-ORGAO_MUN_IBGE'         , 0),   #  <-- TABELA SERVENTIA
  'COD_LOCALIDADE':          ('08-COD_LOCALIDADE'         , 0),   #  <-- TABELA SERVENTIA
  'CLASSE_PROCESSUAL':       ('09-CLASSE_PROCESSUAL'      , 0),
  'TIPO_PROCESSO':           ('10-TIPO_PROCESSO'          , 0),
  'SIST_ELETRONICO':         ('11-SIST_ELETRONICO'        , 0),   #  <-- DE-PARA SISTEMA

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
         f_log.write('  MODULO 5:    CORRECAO E ENRIQUECIMENTO  ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('  CNJ Inova -  HACKATHON - Desafio 2    ' + '\n')
         f_log.write('  CleaNJud  -  Saneamento do DATAJUD    ' + '\n')
         f_log.write('  Version:     20/10/2020 - v 1.0.0     ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('Start at        : ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n')
         f_log.write('Input  Directory: ' + dir_input + '\n')
         f_log.write('Output Directory: ' + dir_output + '\n')

     if (step == 'close'):
         f_log.write('Registros Processados= ' + str(count_processed) + '\n' )
         f_log.write('Completo:  ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n\n')
         f_log.write('\n')
         f_log.write('Resultado Quantitativo das Correcoes FEITAS :' '\n\n')
         espacos = "            "
         for key in reg_output:
            f_log.write(reg_output[key][0] + espacos[:25 -len(reg_output[key][0])] + " = " + str(reg_output[key][1]) + '\n')


     if (step == 'error'):
           f_log.write(message + '\n')

     if (step == 'message'):
           f_log.write(message + '\n')

     f_log.close()


# --------------------------------------------------------------
# Dicionario DOMINIO do campo: dadosBasicos.dscSistema
# identifica qual o sistema eletrônico que o processo tramita.
# --------------------------------------------------------------

dic_sistema= {
'1'   :  '1-Pje',
'2'   :  '2-Projudi',
'3'   :  '3-SAJ',
'4'   :  '4-EPROC',
'5'   :  '5-Apolo',
'6'   :  '6-Themis',
'7'   :  '7-Libra',
'8'   :  '8-Outros',
}


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
#  Leitura da Tabela de SERVENTIA Hierarquizada
# ----------------------------------------------

def f_carrega_tabela_serventia():

    global dic_serventia
    dic_serventia = {}

    nome_kbnl = 'cleanjud_serventias.csv'
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
                      chave = str(c[0])
                      dic_serventia[chave] = c[1:]



# ----------------------------------------------
#  Leitura da Tabela de DE-PARA do NUM CNJ 7 posicoes
#  para o COD_ORGAO e demais info
# ----------------------------------------------

def f_carrega_de_para_cnj7():

    global dic_de_para_cnj7
    dic_de_para_cnj7 = {}

    nome_kbnl = 'cleanjud_de_para_cnj7_orgao.csv'
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
                      chave = str(c[0])
                      dic_de_para_cnj7[chave] = c[1:]


# ----------------------------------------------
#  Leitura da Tabela de DE-PARA do cod Tribunal
#  para o nome do Sistema utilizado
# ----------------------------------------------

def f_carrega_de_para_tribunal_sistema():

    global dic_de_para_sistema
    dic_de_para_sistema = {}

    nome_kbnl = 'cleanjud_de_para_tribunal_sistema.csv'
    file_kbnl = os.path.join(os.path.abspath(os.path.dirname(__file__)), nome_kbnl)

    if not os.path.isfile(file_kbnl):
        print("Arquivo <" + nome_kbnl + ">  nao encontrado")
        sys.exit(-1)

    fileObj = open(file_kbnl, 'r', encoding="utf-8")
    content = fileObj.read()
    fileObj.close()

    for line in content.split('\n'):
              c = line.split(";")
              if (len(c) > 2):
                      chave = str(c[0])
                      dic_de_para_sistema[chave] = c[1:]



# --------------------------------------------------------------
# Funcao de Valida DATA
# --------------------------------------------------------------

def f_valida_data(data_hora):

            data_hora_final = ''
            data_valida = 1

            mascara = {
            14: '%Y%m%d%H%M%S',
            12: '%Y%m%d%H%M%S',
            10: '%Y%m%d%H%M%S',
            8: '%Y%m%d%H%M%S',
            }

            try:
                data_hora_final = time.strptime(str(data_hora), mascara[len(str(data_hora))])
            except:
                data_valida = 0


            if  (data_valida == 0):
                    try:
                        data_hora_final = datetime.fromtimestamp(int(data_hora) / 1000.0)
                        data_valida = 1
                    except:
                        data_valida = 0


            if  (data_valida == 0):
                    try:
                        data_hora_final = datetime(1970, 1, 1) + timedelta(seconds=int(data_hora) / 1000.0)
                        data_valida = 1
                    except:
                        data_valida = 0


            if  (data_valida == 0):
                    try:
                        data_hora_final = datetime.fromtimestamp(int(data_hora) / 1000.0)
                        data_valida = 1
                    except:
                        data_valida = 0

            return data_valida

# ------------------------------------------------------------------------------
# Funcao de Validacao do NUMERO DO PROCESSOS - FORMATO CNJ
# Resolucao num 65 de 16 de dezembro de 2008
# Calculo do DV pelo algoritmo Módulo 97 Base 10 - conforme Norma ISO 7064:2003
# Calculo DV = 98 menos o resto de (NNNNNNNAAAAJTROOOO x 100 ÷ 97).
# ------------------------------------------------------------------------------

def f_valida_num_processo_CNJ (num_processo):

   num_valido = True

   if (len(num_processo) == 20):
      cnj_sequencial = num_processo[0:7]
      cnj_dig_verif  = int(num_processo[7:9])
      cnj_ano_proc   = int(num_processo[9:13])
      cnj_justica    = num_processo[13:14]
      cnj_tribunal   = num_processo[14:16]
      cnj_orgao_jud  = num_processo[16:20]
      cnj_7posicoes  = num_processo[-7:]

      # Calculo do Digito Verificador
      cnj_check_DV =  98 - ((int(num_processo[0:7] + num_processo[9:20]) * 100 ) % 97)

      if (cnj_ano_proc < 1900):
            num_valido = False

      if ( cnj_dig_verif != cnj_check_DV ):
            num_valido = False

   else:
            num_valido    = False
            cnj_7posicoes = ''
            cnj_ano_proc  = ''

   return num_valido, cnj_ano_proc, cnj_7posicoes


# --------------------------------------------------------------
# Faz a contagem dos Campos Vazios e Campos com ZEROS
# --------------------------------------------------------------

def f_correcao_de_dados (indice_entrada):


       i = indice_entrada

       # Calcula variaveis iniciais
       v_orgao = data_in[i][reg_input['ORGAO_CODIGO']]
       v_num_processo = data_in[i][reg_input['NUM_PROCESSO']]
       v_cnj_num_valido, v_cnj_ano_proc, v_cnj_7posicoes = f_valida_num_processo_CNJ (v_num_processo)

       # Faz a varredura nos campos (colunas)
       for key in reg_output:

          campo = data_in[i][reg_input[key]]

          # Corrige Data Ajuizamento a partir da Data de Distribuicao
          if (key == 'DATA_AJUIZAMENTO'):
               data_valida  =  f_valida_data(campo)
               if ( data_valida == 0 ):
                     if (  data_in[i][reg_input['MOV_026_DISTRIB']] != ''):
                          data_in[i][reg_input['DATA_AJUIZAMENTO']] = data_in[i][reg_input['MOV_026_DISTRIB']]
                          # incrementa contador de campos corrigidos
                          reg_output[key] = (reg_output[key][0], reg_output[key][1] + 1)


          # Corrige Codigo do Orgao a partir da Tabela DE-PARA do NUM PROCESSO CNJ de 7 posicoes
          if (key == 'ORGAO_CODIGO'):
              if (str(campo) not in dic_serventia ):
                    if(v_cnj_num_valido == True):
                          if (v_cnj_7posicoes in dic_de_para_cnj7):
                               data_in[i][reg_input['ORGAO_CODIGO']] =   dic_de_para_cnj7[v_cnj_7posicoes][0]
                               v_orgao =  data_in[i][reg_input['ORGAO_CODIGO']]
                               # incrementa contador de campos corrigidos
                               reg_output[key] = (reg_output[key][0], reg_output[key][1] + 1)


          # Corrige ORGAO_MUN_IBGE a partir da Tabela de SERVENDIAS
          if (key == 'ORGAO_MUN_IBGE'):
              if (len(campo) != 7  and campo != '0000000'):
                    if( str(v_orgao)  in dic_serventia):
                          data_in[i][reg_input['ORGAO_MUN_IBGE']] = dic_serventia[str(v_orgao)][6]
                          # incrementa contador de campos corrigidos
                          reg_output[key] = (reg_output[key][0], reg_output[key][1] + 1)


          # Corrige COD_LOCALIDADE a partir da Tabela de SERVENDIAS
          if (key == 'COD_LOCALIDADE'):
              if (len(campo) != 7  and campo != '0000000'):
                    if( str(v_orgao)  in dic_serventia):
                          data_in[i][reg_input['COD_LOCALIDADE']] = dic_serventia[str(v_orgao)][6]
                          # incrementa contador de campos corrigidos
                          reg_output[key] = (reg_output[key][0], reg_output[key][1] + 1)


          # Corrige COD_LOCALIDADE a partir do DE-PARA Tribunal --> Sistema
          if (key == 'SIST_ELETRONICO'):
               if ( campo  in ('1', '2', '3', '4', '5', '6', '7')  ):
                          data_in[i][reg_input['SIST_ELETRONICO']] = dic_sistema[campo]
               else:
                          v_tribunal = data_in[i][reg_input['SIGLA_TRIBUNAL']]
                          if (v_tribunal in dic_de_para_sistema):
                               data_in[i][reg_input['SIST_ELETRONICO']] = dic_de_para_sistema[v_tribunal][1]
                               # incrementa contador de campos corrigidos
                               reg_output[key] = (reg_output[key][0], reg_output[key][1] + 1)



# --------------------------------------------------------------
# MAIN - Corpo Principal
# --------------------------------------------------------------

# Constantes Iniciais - Parametros   - CONFIGURAR NA INSTALACAO !!!
# ------------------------------------------------------------
dir_input  = 'D:\\CNJ\output\\'     # diretorio de entrada
dir_output = 'D:\\CNJ\output\\'     # diretorio de saida
dir_log    = 'D:\\CNJ\LOG\\'        # diretorio de LOG
# --------------------------------------------------------------


#  Inicializacao Variaveis Globais
count_itens = 0         # numero de arquivos de entrada
count_valid = 0         # qde de registros validos
count_processed = 0     # qde de registros processados
count_files = 0         # qde de arquivos processados


# Faz a abertura dos Arquivos de Saida e LOG
now = datetime.datetime.now()
data_atualizacao = now.strftime("%Y-%m-%d")

# Arquivos de LOG e Arquivo de Saida
arq_log  = dir_log    + 'log_cleanjud_5_correcao_' + now.strftime("%Y_%m_%d__%H%M%S") + '.txt'
arq_in   = dir_output + 'cnj_datajud.txt'
arq_out  = dir_output + 'cnj_datajud_corrigido.txt'

f_log = open(arq_log, 'w', encoding="utf-8") # Abre Arquivo de LOG
f_log.close()
f_out = open(arq_out, 'w', encoding="utf-8") # Abre Arquivo de saida

# Inicia o registro do LOG - Relatorio execucao
f_log_trace("begin","")
print('Inicio execucao : '  + now.strftime("%Y-%m-%d %H:%M:%S") )
print('Arq de Entrada: ' + arq_in )
print('Arq de Saida:   ' + arq_out)

# Faz a leitura pra memoria das Tabelas Auxiliares
# Parametros da Funcao de Comparacao Par-e-Par
print('... carregando parametros...')
f_carrega_tabela_serventia()
f_carrega_de_para_cnj7()
f_carrega_de_para_tribunal_sistema()

# Faz a leitura do Arquivo de Entrada
# Arquivo resultando da consolidacao dos Arquivos JSON CNJ
print('... leitura arquivo de entrada...')
f_leitura_entrada(arq_in)

count_processed = 0

# Faz a varredura no Arquivo de Entrada
# e Gera o Relatorio de Inconsistencias
# identificando erros e anomalias
print('... inicio processamento das CORRECOES - SANEAMENTO...')
for idx_entrada in range(len(data_in)):
   if (len(data_in[idx_entrada]) > 5):
      f_correcao_de_dados(idx_entrada)
      count_processed += 1
   if (count_processed % 20000) == 0:
       print("#", end = '')

print('\n' + '... gravando arquivo SANEADO ...')

# Grava Header - nome dos campos  - Relatorio Inconsistencias
v_header = ''
for nome_campo in reg_input:
     v_header +=  nome_campo  + ";"
f_out.write(v_header + '\n')

for idx_entrada in range(len(data_in)):
   if (len(data_in[idx_entrada]) > 5):
        linha = ''
        for col in range(len(data_in[idx_entrada])) :
              linha +=     data_in[idx_entrada][col] + ";"
        f_out.write(linha + '\n')

# Finaliza o registro do LOG
f_log_trace("close", "")

# Fecha do Arquivo de Saida
f_out.close()

print("DONE!")

