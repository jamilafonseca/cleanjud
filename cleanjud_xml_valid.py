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


import xml.etree.ElementTree as ET   # funcoes de manipulacao do XML
from openpyxl import Workbook        # funcao Grava Excel (pip install openpyxl)

import re                            # funcoes regex
import sys                           # Funcoes de interacao com o sist operacional
import os                            # Funcoes do sist operacional e arquivos
import os.path                       # Funcoes do sist operacional e arquivos
import glob                          # Funcoes Diretorio
import datetime                      # funcoes de data e hora corrente
from datetime import datetime

# -----------------------------------------
#  Import Dicionarios (Array - constantes)
#  (tabelas de dados em memoria)
#  BASES DE CONHECIMENTO
#  para validacao e recomposicao da informacao
# -----------------------------------------

from imp_dic_serventia         import dic_serventia         #  Array de Serventias
from imp_dic_nome_orgao        import dic_nome_orgao        #  Array de Serventias - Nome do Orago Julgador
from imp_dic_ramo_justica      import dic_ramo_justica      #  Array de Ramos da Justica
from imp_dic_cnj3_tribunal     import dic_cnj3_tribunal     #  Array de conversao CNJ3 para Tribunal
from imp_dic_cnj7_orgao        import dic_cnj7_orgao        #  Array de conversao CNJ7 para Orgao
from imp_dic_ibge7             import dic_ibge7             #  Array IBGE Municipios (7 posicoes)
from imp_dic_sistemas          import dic_sistemas          #  Array de Tipo de Sistemas por Tribunal
from imp_dic_par_e_par         import dic_par_e_par         #  Array de Validacao Classe e Assunto por Tribunal
from imp_dic_tribunal          import dic_tribunal          #  Array Sigla Tribunal
from imp_dic_tribunal_ramo_uf  import dic_tribunal_ramo_uf  #  Array DE-PARA Sigla Tribunal para Ramo Justica e UF
from imp_dic_tipo_processo     import dic_tipo_processo     #  Array Tipo Processo (eletronico/fisico) a partir Tribunal




# --------------------------------------------------------------
# Export pra arquivo Excel o detalhe das inconsistencias encontradas
# --------------------------------------------------------------

def f_export_excel(arq_saida):

    # Excel Workbook utilizando a biblioteca  openpyxl
    wb = Workbook()
    ws = wb.active

    ws.append( [ 'CLEANJUD XML VALID' ])

    # Registra Cabecalho com o nome dos Metadados que foram tratados
    ws.append( [ 'num_processo','num_status','num_processo_formatado',
            'procEl','procEl_status','procEl_novo',
            'dscSistema','dscSistema_status','dscSistema_novo',
            'classeProcessual','classeProcessual_status',
            'codigoLocalidade','codigoLocalidade_status','codigoLocalidade_novo',
            'siglaTribunal','siglaTribunal_status','siglaTribunal_novo',
            'grau','grau_status',
            'codigoMunicipioIBGE','codigoMunicipioIBGE_status','codigoMunicipioIBGE_novo',
            'codigoOrgao','codigoOrgao_status','codigoOrgao_novo',
            'nomeOrgao','nomeOrgao_status','nomeOrgao_novo' ])

    for i in range(len(lst_xls)):
           ws.append(lst_xls[i])

    ws.title = 'CleaNJud XML Valid'
    wb.save(arq_saida)
    wb.close()


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
      cnj_7posicoes  = num_processo[13:14] + num_processo[14:16] + num_processo[-4:]
      cnj_3posicoes  = num_processo[13:14] + num_processo[14:16]

      cnj_formatado = '{}-{}.{}.{}.{}.{}'.format(num_processo[:7], num_processo[7:9], num_processo[9:13], num_processo[13:14], num_processo[14:16], num_processo[16:20])

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

   return num_valido, cnj_formatado,  cnj_7posicoes, cnj_3posicoes



# --------------------------------------------------------------
# MAIN - Corpo Principal   - VARREDURA DO XML
# --------------------------------------------------------------

# Leitura de Passagem de Parametros
# argument 0 = Program name
# argument 1 = Nome do Arquivo de Entrada
#
if len(sys.argv) == 0:
        print('Favor indormar o nome do arquivo XML de entrada.')
        print(' ')
        print('exemplo:')
        print(' ')
        print('>> cleanjud_xml_valid.exe  exemplo.xml')
        print(' ')
        exit()

# Nome do Arquivo de Entrada XML
v_arq_entrada = sys.argv[1]


# Arquivos de Saida
v_arq_saida    = v_arq_entrada[:-4] + "_corrigido.xml"
v_arq_excel    = v_arq_entrada[:-4] + "_relatorio.xlsx"

# Inicializa variaveis
now = datetime.now()
lst_xls = []   # Lista para o relatorio em Excel

# Inicia a execucao execucao
print('Inicio execucao : '  + now.strftime("%Y-%m-%d %H:%M:%S") )
print('Arq de Entrada:  ' + v_arq_entrada )
print('Arq de Saida:    ' + v_arq_saida )
print('Relatorio Excel :' + v_arq_excel )
print('...')

# Leitura do XML
root = ET.parse(v_arq_entrada).getroot()

for processo in root:

  # Inicializa variaveis do processo:
  v_ibge7  = ''
  v_uf = ''
  v_tribunal = ''
  v_justica = ''

  # Varredura do Matedados de 1 processo
  for elementos in processo:

      if ('dadosBasicos' in elementos.tag ):

           # Avalia NUM_PROCESSO
           #====================
           #print('numero= ' + elementos.attrib['numero'])
           v_num_processo =  elementos.attrib['numero']
           v_cnj_num_valido, cnj_formatado, cnj_7posicoes, cnj_3posicoes = f_valida_num_processo_CNJ (v_num_processo)

           if (v_cnj_num_valido) :      # Se o Num_Processo_CNJ for valido

              v_num_status = 'OK'

              # Calcula Tribunal e Ramo da Justica a partie do NUM_PROCESSO
              if( cnj_3posicoes in dic_cnj3_tribunal ):
                 v_justica = dic_cnj3_tribunal[ cnj_3posicoes ][2]
                 aux = dic_cnj3_tribunal[ cnj_3posicoes ][3]
                 if (aux != ''):
                     v_tribunal = aux

              # Calcula COD MUNIC IBGE7 e UF a partir do NUM_PROCESSO
              if( cnj_7posicoes in dic_cnj7_orgao ):
                   v_ibge7  = dic_cnj7_orgao[ cnj_7posicoes ][4]
                   v_uf = dic_cnj7_orgao[ cnj_7posicoes ][3]
           else:
               v_num_status = 'ERRO'

           # Avalia Tipo Processo
           #=====================
           #print('procEl= ' + elementos.attrib['procEl'])
           v_procEl =  elementos.attrib['procEl']
           v_procEl_novo = ''
           v_procEl_status = 'OK'
           if ( v_procEl not in ('1', '2') ):
                      v_procEl_status = 'ERRO'
                      if (v_tribunal in dic_tipo_processo):
                          # Corrige informacao
                          v_procEl_status = 'corrigido'
                          v_procEl_novo = dic_tipo_processo[v_tribunal]
                          elementos.set ('procEl', v_procEl_novo)

           # Avalia Sistema Tribunal
           #========================
           #print('dscSistema= ' + elementos.attrib['dscSistema'])
           v_dscSistema = elementos.attrib['dscSistema']
           v_dscSistema_novo = ''
           v_dscSistema_status = 'OK'
           if ( v_dscSistema not in ('1','2','3','4','5','6','7','8') ):
                      v_dscSistema_status = 'ERRO'
                      if (v_tribunal in dic_sistemas):
                          # Corrige informacao
                          v_dscSistema_status = 'corrigido'
                          v_dscSistema_novo = dic_sistemas[v_tribunal][0]
                          elementos.set ('dscSistema', v_dscSistema_novo)

           # Avalia CLASSE PROCESSUAL
           #=========================
           #print('classeProcessual= ' + elementos.attrib['classeProcessual'])
           v_classeProcessual = elementos.attrib['classeProcessual']
           if (v_classeProcessual != ''):
               v_classeProcessual_status = 'OK'
           else:
               v_classeProcessual_status = 'ERRO'

           # Avalia COD LOCALIDADE
           #=========================
           #print('codigoLocalidade= ' + elementos.attrib['codigoLocalidade'])
           v_codigoLocalidade = elementos.attrib['codigoLocalidade']
           v_codigoLocalidade_status = 'OK'
           v_codigoLocalidade_novo = ''
           if ( v_codigoLocalidade not in dic_ibge7 ):
                      v_codigoLocalidade_status = 'ERRO'
                      if (v_ibge7 != ''):
                          # Corrige informacao
                          v_codigoLocalidade_status = 'corrigido'
                          v_codigoLocalidade_novo = v_ibge7
                          elementos.set ('dscSistema', v_codigoLocalidade_novo)


           for dadosbasicos in elementos:

               #print(dadosbasicos.tag)
               #print(dadosbasicos.attrib)
               #print('Assunto= ' + elementos['assunto']['codigoNacional'].text)

              # if ('assunto' in dadosbasicos.tag ):
              #        print('principal= ' + dadosbasicos.attrib['principal'])

               if ('siglaTribunal' in dadosbasicos.tag ):

                       # Valida SIGLA TRIBUNAL
                       # =====================
                       #print('siglaTribunal= ' + dadosbasicos.text)
                       v_siglaTribunal =  dadosbasicos.text
                       v_siglaTribunal_novo = ''
                       v_siglaTribunal_status = 'OK'
                       if ( v_siglaTribunal not in dic_tribunal ):
                            v_siglaTribunal_status = 'ERRO'
                       if ( v_tribunal != '' and v_tribunal != v_siglaTribunal):
                                  # Corrige informacao
                                  v_siglaTribunal_status = 'corrigido'
                                  v_siglaTribunal_novo = v_tribunal
                                  dadosbasicos.text = v_tribunal


               if ('grau' in dadosbasicos.tag ):

                       # Valida GRAU JURISDICAO
                       # ===================#==
                       #print('grau= ' + dadosbasicos.text)
                       v_grau =  dadosbasicos.text
                       v_grau_status = 'OK'
                       if ( v_grau.upper() not in ('G1','G2','SUP','JE','TEU','TR','TRU','TNU','CSJT','CJF','CNJ')):
                            v_grau_status = 'ERRO'

                       # Avalia novamente a CLASSE PROCESSUAL
                       # batendo com o Grau+Ramo Justica
                       #===================================
                       if ( ('CLASSE_PROCESSUAL','JUSTICA','GRAU_JURISDICAO', v_classeProcessual, v_justica, v_grau) in dic_par_e_par):
                            retorno = dic_par_e_par['CLASSE_PROCESSUAL','JUSTICA','GRAU_JURISDICAO', v_classeProcessual, v_justica, v_grau]
                            if (retorno == 'N')  :
                                 v_classeProcessual_status = "Inconsistente"


               if ('orgaoJulgador' in dadosbasicos.tag ):

                        # Avalia COD MUNIC IBGE
                        #=========================
                        #print('codigoMunicipioIBGE= ' + dadosbasicos.attrib['codigoMunicipioIBGE'] )
                        v_codigoMunicipioIBGE = dadosbasicos.attrib['codigoMunicipioIBGE']
                        v_codigoMunicipioIBGE_status = 'OK'
                        v_codigoMunicipioIBGE_novo = ''
                        if ( v_codigoMunicipioIBGE not in dic_ibge7 ):
                             v_codigoMunicipioIBGE_status = 'ERRO'
                             if (v_ibge7 != ''):
                                 # Corrige informacao
                                 v_codigoMunicipioIBGE_status = 'corrigido'
                                 v_codigoMunicipioIBGE_novo = v_ibge7
                                 dadosbasicos.set ('codigoMunicipioIBGE', v_codigoMunicipioIBGE_novo)

                        # Avalia COD ORGAO
                        #=========================
                        #print('codigoOrgao= ' + dadosbasicos.attrib['codigoOrgao'])
                        v_codigoOrgao = dadosbasicos.attrib['codigoOrgao']
                        v_codigoOrgao_status = 'OK'
                        v_codigoOrgao_novo = ''
                        if ( v_codigoOrgao not in dic_serventia ):
                             v_codigoOrgao_status = 'ERRO'
                             if( cnj_7posicoes in dic_cnj7_orgao):
                                 # Corrige informacao
                                 v_codigoOrgao_status = 'corrigido'
                                 v_codigoOrgao_novo = dic_cnj7_orgao[cnj_7posicoes][0]
                                 dadosbasicos.set ('codigoOrgao', v_codigoOrgao_novo)


                        # Avalia NOME ORGAO
                        #=========================
                        #print('nomeOrgao= ' + dadosbasicos.attrib['nomeOrgao'])
                        v_nomeOrgao = dadosbasicos.attrib['nomeOrgao']
                        v_nomeOrgao_status = 'OK'
                        v_nomeOrgao_novo = ''
                        if ( v_nomeOrgao.strip() == '' or v_nomeOrgao in ('vazio', 'NONE', 'none','None','null','NULL', 'ND','N/D', 'OUTROS') ):
                             v_nomeOrgao_status = 'ERRO'
                             if( cnj_7posicoes in dic_cnj7_orgao):

                                 # Corrige informacao
                                 if (v_codigoOrgao_status == 'OK'):
                                      v_orgao = v_codigoOrgao
                                 else:
                                      v_orgao = v_codigoOrgao_novo

                                 if (v_orgao in dic_nome_orgao):
                                     v_nomeOrgao_status = 'corrigido'
                                     v_nomeOrgao_novo = dic_nome_orgao[v_orgao]
                                     dadosbasicos.set ('nomeOrgao', v_nomeOrgao_novo)


  # -----------  Final do LOOP de 1 processo

  # Registra as informacoes para o relatorio em EXCEL
  # =================================================
  lst_xls.append ([v_num_processo,v_num_status,cnj_formatado,
                   v_procEl,v_procEl_status,v_procEl_novo,
                   v_dscSistema,v_dscSistema_status,v_dscSistema_novo,
                   v_classeProcessual,v_classeProcessual_status,
                   v_codigoLocalidade,v_codigoLocalidade_status,v_codigoLocalidade_novo,
                   v_siglaTribunal,v_siglaTribunal_status,v_siglaTribunal_novo,
                   v_grau,v_grau_status,
                   v_codigoMunicipioIBGE,v_codigoMunicipioIBGE_status,v_codigoMunicipioIBGE_novo,
                   v_codigoOrgao,v_codigoOrgao_status,v_codigoOrgao_novo,
                   v_nomeOrgao,v_nomeOrgao_status,v_nomeOrgao_novo ])


# ------- Final do LOOP de todos os processos do XML de entrada


# Grava o NOVO XML corrigido
# ===========================
print('... Gravando o Arquivo XML corrigido...' + '\n')
saida = ET.tostring(root, encoding='utf8').decode('utf8')
saida  = re.sub('ns1:','', saida)

with open(v_arq_saida,'w', encoding="utf-8") as f_out:
    f_out.write(saida)
    f_out.close()

# Grava o relatorio em EXCEL
# ===========================
print('... Gravando o Relatorio em Excel...' + '\n')
f_export_excel(v_arq_excel)

now = datetime.now()
print('Final execucao : '  + now.strftime("%Y-%m-%d %H:%M:%S") )