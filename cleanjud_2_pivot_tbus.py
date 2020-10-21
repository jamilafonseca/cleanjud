#-------------------------------------------------------------------------------
# CNJ Inova -  HACKATHON - Desafio 2 - #Saneamento de Dados
#-------------------------------------------------------------------------------
# SOLUCAO:     CleaNJud - Decisoes mais Eficazes com a Qualidade do DATAJUD
#-------------------------------------------------------------------------------
# Purpose:     MODULO 2 - Cria Tabela unificada das TBUs (Classe e Assunto)
#              - Faz a Leitura de Arquivos CSV das tabelas Unicas
#              - Faz o Pivot (transposicao de colunas em linhas)
#              - converte a coluna em:  JUSTICA + Grau Jurisdicao
#              - Grava a base pra ser utilizada no Rel de Inconsistencias
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

import sys               # Funcoes de interacao com o sist operacional
import os                # Funcoes do sist operacional e arquivos
import os.path           # Funcoes do sist operacional e arquivos
import glob              # Funcoes Diretorio
import datetime          # funcoes de data e hora corrente

# ----------------------------------------------------------------------
#  Dicionario de Colunas das Tabelas Unificadas (Mov, Classe e Assunto)
#  para transposicao (pivot) das colunas em linhas
#  Converte:  Nome_coluna -->  (Nome Justiça, Grau Jurisdicao)
# ----------------------------------------------------------------------

dic_tabelas_unificadas = {
      'JUST_ES_1GRAU' :          ( 'ESTADUAL', 'G1' ),
      'JUST_ES_2GRAU' :          ( 'ESTADUAL', 'G2' ),
      'JUST_ES_JUIZADO_ES' :     ( 'ESTADUAL', 'JE' ),
      'JUST_ES_TURMAS' :         ( 'ESTADUAL', 'TEU' ),
      'JUST_ES_1GRAU_MIL' :      ( 'TJM',      'G1' ),
      'JUST_ES_2GRAU_MIL' :      ( 'TJM',      'G2' ),
      'JUST_ES_JUIZADO_ES_FP' :  ( 'FAZENDA',  'JE' ),
      'JUST_TU_ES_UN' :          ( 'ESTADUAL', 'TR' ),
      'JUST_FED_1GRAU' :         ( 'FEDERAL',  'G1' ),
      'JUST_FED_2GRAU' :         ( 'FEDERAL',  'G2' ),
      'JUST_FED_JUIZADO_ES' :    ( 'FEDERAL',  'JE' ),
      'JUST_FED_TURMAS' :        ( 'FEDERAL',  'TEU' ),
      'JUST_FED_NACIONAL' :      ( 'FEDERAL',  'TNU' ),
      'JUST_FED_REGIONAL' :      ( 'FEDERAL',  'TRU' ),
      'JUST_TRAB_1GRAU' :        ( 'TRABALHO', 'G1' ),
      'JUST_TRAB_2GRAU' :        ( 'TRABALHO', 'G2' ),
      'JUST_TRAB_TST' :          ( 'TRABALHO', 'SUP' ),
      'JUST_TRAB_CSJT' :         ( 'TRABALHO', 'CSJT' ),
      'STF' :                    ( 'STF', 'SUP' ),
      'STJ' :                    ( 'STJ', 'G1' ),
      'STJ' :                    ( 'STJ', 'G2' ),
      'STJ' :                    ( 'STJ', 'SUP' ),
      'CJF' :                    ( 'CJF', 'CJF' ),
      'CNJ' :                    ( 'CNJ', 'CNJ' ),
      'JUST_MIL_UNIAO_1GRAU' :   ( 'MILITAR UNIAO',    'G1' ),
      'JUST_MIL_UNIAO_STM' :     ( 'MILITAR UNIAO',    'SUP' ),
      'JUST_MIL_EST_1GRAU' :     ( 'MILITAR ESTATUAL', 'G1' ),
      'JUST_MIL_EST_TJM' :       ( 'MILITAR ESTATUAL', 'SUP' ),
      'JUST_ELEI_1GRAU' :        ( 'ELEITORAL', 'G1' ),
      'JUST_ELEI_2GRAU' :        ( 'ELEITORAL', 'G2' ),
      'JUST_ELEI_TSE' :          ( 'ELEITORAL', 'SUP' ),
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
         f_log.write('  MODULO 2:    Unifica Tableas Classe e Assunto   ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('  CNJ Inova -  HACKATHON - Desafio 2    ' + '\n')
         f_log.write('  CleaNJud  -  Saneamento do DATAJUD    ' + '\n')
         f_log.write('  Version:     11/10/2020 - v 1.0.0     ' + '\n')
         f_log.write('------------------------------------------------' + '\n')
         f_log.write('Start at        : ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n')
         f_log.write('Input  Directory: ' + dir_input + '\n')
         f_log.write('Output Directory: ' + dir_output + '\n')

     if (step == 'close'):
         f_log.write('Completo:  ' + now.strftime("%Y-%m-%d %H:%M:%S") + '\n\n')
         campo = ''

     if (step == 'error'):
           f_log.write(message + '\n')

     if (step == 'message'):
           f_log.write(message + '\n')

     f_log.close()


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



# ---------------------------------
#   Faz a Varredura da Tabela e
#   a transposição das Colunas (Pivot)
# ---------------------------------

def f_pivot_table(nome_tabela):

    linha = -1

    for key in data_in:

        linha +=1
        for col in range(len(data_in[linha])-1):

             v_coluna = header_in[0][col + 1]
             v_justica = dic_tabelas_unificadas[v_coluna][0]
             v_grau    = dic_tabelas_unificadas[v_coluna][1]
             v_conteudo = data_in[linha][col + 1]

             if (v_conteudo in ('S','s')):
                  v_conteudo = 'S'
             else:
                  v_conteudo = 'N'

             reg_saida = nome_tabela + ';'
             reg_saida += 'JUSTICA;GRAU_JURISDICAO;'
             reg_saida +=  data_in[linha][0] + ';'
             reg_saida += v_justica + ';' + v_grau + ';'
             reg_saida += v_conteudo

             f_out.write( reg_saida +'\n')


# --------------------------------------------------------------
# MAIN - Corpo Principal
# --------------------------------------------------------------

# Constantes Iniciais - Parametros   - CONFIGURAR NA INSTALACAO !!!
# ------------------------------------------------------------
dir_input  = 'C:\\bases_CNJ'        # diretorio de entrada
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
arq_log       = dir_log    + 'log_cleanjud_2_tbu_' + now.strftime("%Y_%m_%d__%H%M%S") + '.txt'
arq_out       = dir_output + 'cleanjud_par_e_par.csv'

f_log = open(arq_log, 'w', encoding="utf-8") # Abre Arquivo de LOG
f_log.close()
f_out = open(arq_out, 'w', encoding="utf-8") # Abre Arquivo de saida


# Inicia o registro do LOG - Relatorio execucao
f_log_trace("begin","")
print('Inicio execucao : '  + now.strftime("%Y-%m-%d %H:%M:%S") )

# Processa tabela SGT CLASSES
nome_kbnl = 'cleanjud_sgt_classe.csv'
file_kbnl = os.path.join(os.path.abspath(os.path.dirname(__file__)), nome_kbnl)
f_leitura_entrada(file_kbnl)
f_pivot_table('CLASSE_PROCESSUAL')


# Processa tabela SGT ASSUNTOS
nome_kbnl = 'cleanjud_sgt_assunto.csv'
file_kbnl = os.path.join(os.path.abspath(os.path.dirname(__file__)), nome_kbnl)
f_leitura_entrada(file_kbnl)
f_pivot_table('ASSUNTO1_COD_NACIONAL')



# Finaliza o registro do LOG
f_log_trace("close", "")

# Fecha do Arquivo de Saida
f_out.close()

print("DONE!")