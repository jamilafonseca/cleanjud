#-------------------------------------------------------------------------------
# CNJ Inova -  HACKATHON - Desafio 2 - #Saneamento de Dados
#-------------------------------------------------------------------------------
# SOLUCAO:     CleaNJud - Decisoes mais Eficazes com a Qualidade do DATAJUD
#-------------------------------------------------------------------------------
# Purpose:     MODULO 6 - ChatBot para o WhatsApp
#              - Responde duvidas cadastrais dos Advogados,
#                tentando evitar a entrada de informacoes inconsistentes
#                no cadastro dos diversos sistemas dos Tribunais.
#              - A partir de alguns termos, utilizando tecnicas NLP
#                o BOT procura e mostra o COD_ASSUNTO_NACIONAL;
#              - A partir do NUM_PROCESSO, o BOT informa
#                COD_ORGAO, COD_IBGE, Localizações, etc.
#-------------------------------------------------------------------------------
#  OBSERVACAO:
#              Esse codigo foi BASEADO nos projetos GitHub publicos abaixo:
#              https://github.com/jonathanferreiras/whats-bot
#              https://medium.com/@jonathanferreiras/chatbot-python-whatsapp-e9c1079da5a
#
#
#              Foram aproveitados APENAS os trechos de codigo basicos (comandos)
#              de integracao com o WhatsApp via biblioteca SELENIUM.
#
#              As interacoes, respostas e conteudos do CHATBOT foram
#              criados pelo Grupo.
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

import re                # funcoes regex
import sys               # Funcoes de interacao com o sist operacional
import os                # Funcoes do sist operacional e arquivos
import os.path           # Funcoes do sist operacional e arquivos
import glob              # Funcoes Diretorio
import datetime          # funcoes de data e hora corrente
import time              # funcoes de data e hora corrente
from bot import wppbot           # biblioteca LOCAL

import requests
import json
#from chatterbot.trainers import ListTrainer
#from chatterbot import ChatBot
from selenium import webdriver




# ---------------------------------------------------
#    Programa Principal
#----------------------------------------------------

bot = wppbot('CleaNJud')
bot.inicia('CleaNJud')
bot.saudacao(['Olá, sou o BOT da CleanJud!','Em que posso ajudar?'])
ultimo_texto = ''



while True:

    texto = bot.escuta()

    if texto != ultimo_texto :

        ultimo_texto = texto
        texto = texto.lower()

        if (texto in ('assunto','codigo')):
            bot.responde('tabela de assuntos')
        elif (texto in ('0','1','2','3','4','5','6','7','8','9')):
            bot.responde('numero oo processo')
        else:
            bot.responde('Ops... não entendi. pode explicar de outra maneira?')




