#-------------------------------------------------------------------------------
# CNJ Inova -  HACKATHON - Desafio 2 - #Saneamento de Dados
#-------------------------------------------------------------------------------
# SOLUCAO:     CleaNJud - Decisoes mais Eficazes com a Qualidade do DATAJUD
#-------------------------------------------------------------------------------
# Purpose:     MODULO 3 - Relatorio INCONSISTENCIAS
#              - Faz a contagem de Campos com valores Vazios e Zerados
#              - Faz teste de Data e de Digito Verificador
#              - Faz teste de consistencia de Dominio dos Dados
#              - Faz teste de consistencia com Tabelas do SGT (Classe e Assunto)
#              - Faz teste de consistencia das Movimentacoes e das Datas
#              - Grava Relatorio de Inconsitencias Consolidado (Arquivo CSV)
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

reg_output = {
  'DATA_AJUIZAMENTO':        ('01-DATA_AJUIZAMENTO'       , 0),
  'NUM_PROCESSO':            ('02-NUM_PROCESSO'           , 0),
  'GRAU_JURISDICAO':         ('03-GRAU_JURISDICAO'        , 0),
  'SIGLA_TRIBUNAL':          ('04-SIGLA_TRIBUNAL'         , 0),
  'ORGAO_NOME':              ('05-ORGAO_NOME'             , 0),
  'ORGAO_CODIGO':            ('06-ORGAO_CODIGO'           , 0),
  'ORGAO_MUN_IBGE':          ('07-ORGAO_MUN_IBGE'         , 0),
  'COD_LOCALIDADE':          ('08-COD_LOCALIDADE'         , 0),
  'CLASSE_PROCESSUAL':       ('09-CLASSE_PROCESSUAL'      , 0),
  'TIPO_PROCESSO':           ('10-TIPO_PROCESSO'          , 0),
  'SIST_ELETRONICO':         ('11-SIST_ELETRONICO'        , 0),
  'VALOR_CAUSA':             ('12-VALOR_CAUSA'            , 0),
  'ASSUNTO1_COD_LOCAL':      ('13-ASSUNTO1_COD_LOCAL'     , 0),
  'ASSUNTO1_COD_PAI':        ('14-ASSUNTO1_COD_PAI'       , 0),
  'ASSUNTO1_DESCRICAO':      ('15-ASSUNTO1_DESCRICAO'     , 0),
  'ASSUNTO1_COD_NACIONAL':   ('16-ASSUNTO1_COD_NACIONAL'  , 0),
  'MOV_026_DISTRIB':         ('17-MOV_026_DISTRIBUICAO'   , 0),
  'MOV_193_SENTENCA':        ('18-MOV_193_SENTENCA'       , 0),
  'MOV_848_JULGADO':         ('19-MOV_848_TRANSITO_JULGADO'  , 0),
  'MOV_022_BAIXA':           ('20-MOV_022_BAIXA'          , 0),
  'MOV_246_ARQUIVA':         ('21-MOV_246_ARQUIVADO'      , 0),
  'MOV_NAC_TODOS':           ('22-MOV_NACIONAL_TODOS'     , 0),
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
         f_log.write('  MODULO 3:    Relat Inconsistencias   ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('  CNJ Inova -  HACKATHON - Desafio 2    ' + '\n')
         f_log.write('  CleaNJud  -  Saneamento do DATAJUD    ' + '\n')
         f_log.write('  Version:     16/10/2020 - v 1.0.0     ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('Start at        : ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n')
         f_log.write('Input  Directory: ' + dir_input + '\n')
         f_log.write('Output Directory: ' + dir_output + '\n')

     if (step == 'close'):
         f_log.write('Registros Processados= ' + str(count_processed) + '\n' )
         f_log.write('Completo:  ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n\n')

     if (step == 'error'):
           f_log.write(message + '\n')

     if (step == 'message'):
           f_log.write(message + '\n')

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
#  Leitura da Tabela de SERVENTIA Hierarquizada
# ----------------------------------------------

def f_carrega_tabela_serventia():

    global dic_hierarquia_serventia
    dic_hierarquia_serventia = {}

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
                      dic_hierarquia_serventia[chave] = c[1:]




# -----------------------------------------------------------------------
#  Leitura do Dicionario de Validação PAR com PAR
#  Teste de Inconsistencia Conceitual - Analise de Contexto da informacao
# -----------------------------------------------------------------------

def f_dic_par_e_par_read():

    global dic_par_e_par
    dic_par_e_par = {}

    nome_kbnl = 'cleanjud_par_e_par.csv'
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
                  if (len(c) > 5):
                      dic_par_e_par[c[0],c[1],c[2],c[3],c[4],c[5]] = c[6]
                  else:
                      dic_par_e_par[c[0],c[1],c[2],c[3]] = c[4]

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
      cnj_7posicoes  = '{}.{}.{}'.format(num_processo[13:14], num_processo[14:16], num_processo[-4:])

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

def f_monta_rel_inconsistencias( indice_entrada):


       i = indice_entrada

       # Calcula variaveis iniciais
       v_orgao = data_in[i][reg_input['ORGAO_CODIGO']]
       v_justica = f_calc_tipo_justica(data_in[i][reg_input['SIGLA_TRIBUNAL']] )
       v_grau = str(data_in[i][reg_input['GRAU_JURISDICAO']] )
       v_grau = v_grau.upper()
       v_classe = data_in[i][reg_input['CLASSE_PROCESSUAL']]
       v_num_processo = data_in[i][reg_input['NUM_PROCESSO']]
       v_cnj_num_valido, v_cnj_ano_proc, v_cnj_7posicoes = f_valida_num_processo_CNJ (v_num_processo)


       for key in reg_output:

          campo = data_in[i][reg_input[key]]
          inc_vazio = 0
          inc_zero = 0
          inc_invalido = 0
          inc_inconsistente = 0
          status_valido = 1

          # Faz a contagem de Campos VAZIOS
          #-------------------------------------
          if (campo in ('', ' ', 'NONE', 'none','None','null','NULL', 'ND','N/D')):
               inc_vazio = 1
               status_valido = 0

          # Faz a contagem de Campos ZERADOS
          #-------------------------------------
          if (campo in ('0','00', '000', '0000', '00000' , '000000' ,'0.00', '0.0', '0,0', '0,00')):
               inc_zero = 1
               status_valido = 0

          # Faz a contagem de Campos INVALIDOS
          # valida estrutura dos campos
          # e valida digito verificador
          #-------------------------------------
          if (key == 'DATA_AJUIZAMENTO'):
               data_valida  =  f_valida_data(campo)
               if ( data_valida == 0 ):
                     inc_invalido = 1
                     status_valido = 0

          if (key in ('MOV_026_DISTRIB','MOV_193_SENTENCA','MOV_848_JULGADO','MOV_022_BAIXA','MOV_246_ARQUIVA')):
               if (status_valido == 1)   :
                  data_valida  =  f_valida_data(campo)
                  if ( data_valida == 0 ):
                       inc_invalido = 1
                       status_valido = 0

          if (key == 'GRAU_JURISDICAO'):
               if ( campo.upper() not in ('G1','G2','SUP', 'JE','TEU','TR','TRU','TNU','CSJT','CJF','CNJ')  and status_valido == 1):
                     inc_invalido = 1
                     status_valido = 0

          if (key == 'NUM_PROCESSO'):
               if ( not v_cnj_num_valido ):
                     inc_invalido = 1
                     status_valido = 0

          if (key == 'VALOR_CAUSA'):
              if (campo != '' and status_valido == 1):
                 if float(campo) < 50:
                     inc_invalido = 1
                     status_valido = 0

          if (key == 'ORGAO_CODIGO'):
              if (str(campo) not in dic_hierarquia_serventia ):
                     inc_invalido = 1
                     status_valido = 0

          if (key in ('ORGAO_MUN_IBGE', 'COD_LOCALIDADE')):
            if (len(campo) != 7 and status_valido == 1):
                     inc_invalido = 1
                     status_valido = 0

          if (key == 'TIPO_PROCESSO'):        # 1: Sistema Eletrônico - 2: Sistema Físico
               if ( campo not in ('1', '2')  and status_valido == 1):
                     inc_invalido = 1
                     status_valido = 0


          # Faz a contagem de Campos INCONSISTENTES
          # atraves de batimento de informacoes
          #-------------------------------------

          if (key == 'CLASSE_PROCESSUAL'  and status_valido == 1):

              if ( ('CLASSE_PROCESSUAL','JUSTICA','GRAU_JURISDICAO', campo, v_justica, v_grau) in dic_par_e_par):
                 retorno = dic_par_e_par['CLASSE_PROCESSUAL','JUSTICA','GRAU_JURISDICAO', campo, v_justica, v_grau]
                 if (retorno == 'N')  :
                    inc_inconsistente = 1
                    status_valido = 0
              else:
                    inc_inconsistente = 1
                    status_valido = 0

          if (key == 'ASSUNTO1_COD_NACIONAL'  and status_valido == 1):

              if ( ('ASSUNTO1_COD_NACIONAL','JUSTICA','GRAU_JURISDICAO', campo, v_justica, v_grau) in dic_par_e_par):
                 retorno = dic_par_e_par['ASSUNTO1_COD_NACIONAL','JUSTICA','GRAU_JURISDICAO', campo, v_justica, v_grau]
                 if (retorno == 'N')  :
                    inc_inconsistente = 1
                    status_valido = 0
              else:
                    inc_inconsistente = 1
                    status_valido = 0


          # Valida a Sequencia Correta das Movimentacoes do Ciclo Processual

          if (key == 'MOV_246_ARQUIVA' and  status_valido == 1):
              if (data_in[i][reg_input['MOV_022_BAIXA']] == '') or (data_in[i][reg_input['MOV_022_BAIXA']] > campo) :
                    inc_inconsistente = 1
                    status_valido = 0

          if (key == 'MOV_246_ARQUIVA' and  status_valido == 1):
              if (data_in[i][reg_input['MOV_848_JULGADO']] == '') or (data_in[i][reg_input['MOV_848_JULGADO']] > campo) :
                    inc_inconsistente = 1
                    status_valido = 0

          if (key == 'MOV_246_ARQUIVA' and  status_valido == 1):
              if (data_in[i][reg_input['MOV_193_SENTENCA']] == '') or (data_in[i][reg_input['MOV_193_SENTENCA']] > campo) :
                    inc_inconsistente = 1
                    status_valido = 0

          if (key == 'MOV_022_BAIXA ' and  status_valido == 1):
              if (data_in[i][reg_input['MOV_848_JULGADO']] == '') or (data_in[i][reg_input['MOV_848_JULGADO']] > campo) :
                    inc_inconsistente = 1
                    status_valido = 0

          if (key == 'MOV_848_JULGADO ' and  status_valido == 1):
              if (data_in[i][reg_input['MOV_193_SENTENCA']] == '') or (data_in[i][reg_input['MOV_193_SENTENCA']] > campo) :
                    inc_inconsistente = 1
                    status_valido = 0

          if (key == 'MOV_193_SENTENCA ' and  status_valido == 1):
              if (data_in[i][reg_input['MOV_026_DISTRIB']] == '') or (data_in[i][reg_input['MOV_026_DISTRIB']] > campo) :
                    inc_inconsistente = 1
                    status_valido = 0


          #registra status geral de validade no campo de saida
         # reg_output[key] = (campo, status_valido)

          #registra os contadores no relatorio de inconsistencias

          if v_orgao == '':
             v_orgao = 'vazio'

          k = reg_output[key][0]

          if (v_orgao,k) in relat:
              relat[v_orgao,k] = (relat[v_orgao,k][0]+1,relat[v_orgao,k][1]+status_valido,relat[v_orgao,k][2]+inc_vazio,relat[v_orgao,k][3]+inc_zero,relat[v_orgao,k][4]+inc_invalido,relat[v_orgao,k][5]+inc_inconsistente)
          else:
              relat[v_orgao,k] = (1,status_valido,inc_vazio,inc_zero,inc_invalido,inc_inconsistente)

       return


# -------------------------------------
#   Registra na Memoria campo de saida
# -------------------------------------

def f_registra_mem(key, conteudo):

    if (len(conteudo) > 0):
         inc = 1
    else:
         inc = 0

    conteudo = conteudo.translate(bytes.maketrans(b"|?;()", b"     "))   # retira delimitadores
    conteudo = re.sub('\s+',' ', conteudo) # retira espacos duplos desnecessarios
    conteudo = re.sub('"', ' ', conteudo)  # retira aspas duplas
    conteudo = re.sub("'", ' ', conteudo)  # retira aspas simples
    conteudo = re.sub('\n', ' ', conteudo) # retira quebra de linha

    reg_output[key] = (conteudo, reg_output[key][1] + inc )



# --------------------------------------------------------------
# MAIN - Corpo Principal
# --------------------------------------------------------------

# Constantes Iniciais - Parametros   - CONFIGURAR NA INSTALACAO !!!
# ------------------------------------------------------------
dir_input  = 'D:\\CNJ\output\\'     # diretorio de entrada
dir_output = 'D:\\CNJ\output\\'     # diretorio de saida
dir_log    = 'D:\\CNJ\LOG\\'        # diretorio de LOG
# ------------------------------------------------------------

#  Inicializacao Variaveis Globais
count_itens = 0         # numero de arquivos de entrada
count_valid = 0         # qde de registros validos
count_processed = 0     # qde de registros processados
count_files = 0         # qde de arquivos processados


# Faz a abertura dos Arquivos de Saida e LOG
now = datetime.datetime.now()
data_atualizacao = now.strftime("%Y-%m-%d")

# Arquivos de LOG e Arquivo de Saida
arq_log  = dir_log    + 'log_cleanjud_3_relat_' + now.strftime("%Y_%m_%d__%H%M%S") + '.txt'
arq_in   = dir_output + 'cnj_datajud.txt'
arq_out  = dir_output + 'cnj_rel_inconsistencia.txt'

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
f_dic_par_e_par_read()
f_carrega_tabela_serventia()

# Faz a leitura do Arquivo de Entrada
# Arquivo resultando da consolidacao dos Arquivos JSON CNJ
print('... leitura arquivo de entrada...')
f_leitura_entrada(arq_in)

relat = {}
count_processed = 0

# Faz a varredura no Arquivo de Entrada
# e Gera o Relatorio de Inconsistencias
# identificando erros e anomalias
print('... inicio processamento relatorio inconsistencias...')
for idx_entrada in range(len(data_in)):
   if (len(data_in[idx_entrada]) > 5):
      f_monta_rel_inconsistencias(idx_entrada)
      count_processed += 1
   if (count_processed % 29000) == 0:
       print("#", end = '')

print('\n' + '... gravando arquivo de relatorio ...')

# Grava Header - nome dos campos  - Relatorio Inconsistencias
v_header = "ORGAO_CODIGO;TRIBUNAL;TRIBUNAL_JUSTICA;NOME_ORGAO;MUNICIPIO;UF;NOME_CAMPO;QDE_TOTAL;QDE_VALIDOS;QDE_VAZIOS;QDE_COM_ZEROS;QDE_ERROS;QDE_INCONSISTENCIA;"
f_out.write(v_header + '\n')


# Write File - Grava no Arquivo CSV o relatorio de inconsistencia
for orgao,key in relat:
   v_linha = orgao + ";"
   if (str(orgao) in dic_hierarquia_serventia ):
       v_linha += dic_hierarquia_serventia[str(orgao)][0] + ";"  # TRIBUNAL
       v_linha += dic_hierarquia_serventia[str(orgao)][2] + ";"  # TRIBUNAL_JUSTICA
       v_linha += dic_hierarquia_serventia[str(orgao)][10] + ";"  # NOME_ORGAO
       v_linha += dic_hierarquia_serventia[str(orgao)][4] + ";"  # MUNICIPIO
       v_linha += dic_hierarquia_serventia[str(orgao)][5] + ";"  # UF
   else:
       v_linha += 'OUTROS;OUTROS;OUTROS;OUTROS;OUTROS;'
   v_linha += key + ";"
   for i in range(len(relat[orgao,key])):
        v_linha += str(relat[orgao,key][i]) + ";"
   f_out.write(v_linha + '\n')

# Finaliza o registro do LOG
f_log_trace("close", "")

# Fecha do Arquivo de Saida
f_out.close()

print("DONE!")
