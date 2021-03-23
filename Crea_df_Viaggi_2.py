# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 12:07:35 2020

@author: asus pc
"""

import os
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.pyplot import *
import Routine_Viaggi
import time
from Routine_Viaggi import Crea_df_Viaggi_Vuoto
from Routine_Viaggi import Crea_Vett_x_INFO_Veic
from Routine_Viaggi import tempo
from Routine_Viaggi import Setta_Fl_Viaggio
from  Routine_Viaggi import Crea_Id_Viaggi
from Routine_Viaggi import Tab_Sintetica
from Routine_Viaggi import Setta_Soste_Generiche_Notte_e_Giorno
import db_connect


def Setta_Fl_Viaggio(dati1v):
    # ==================================================================
    #   Calcola e Restituisce:  il vettore:   Fl_Viag
    #
    #   Gli elementi di questo vettore sono flag che indicano se il
    #   viaggio inizia dall'interno o entra dal confine,
    #   o è la prima apparizione temporale.
    #   Analogamente x il fine viaggio
    # ==================================================================
    #   Inut: nessuno. ( Utilizza "dati1v" )
    #   Output: vettore Fl_Viag
    #           Veic_da_Eliminare ( ==0: non va eliminato
    #                               !=0: VA ELIMINATO
    #                               Il suo valore indica la causa dell'eliminaz.
    #                               (vedere routine: "Aggiungi_1Veic_Eliminato")
    # ==================================================================
    #                         Codici         Fl_Viag
    #    5      Sosta irregolare. Accende il motore per un pò e lo rispegne
    #   10      Inizio regolare
    #   11      Inizio da confine
    #   12      Inizio temoporale
    #   13      Inizio temporale + Inizio da confine
    #   20      Fine regolare
    #   21      Fine da confine
    #   22      Fine temoporale
    #   23      Fine temporale + Fine da confine
    # ==================================================================
    #
    #             St.Motore   Evento
    #                 1        56
    # Spegnimento     0         7
    # Accensione      1         6
    #

    Fl_Viag = np.zeros(len(dati1v), dtype='int')
    # ...........................
    _Veic_da_Eliminare = 0

    # ==================================================================
    # ______ calcola:   INIZIO-FINE     TEMPORALE1  ____________________
    # ==================================================================
    k = 0  # ______________________________ 1° registrazione
    if ((dati1v.longitude[k] < Lon_Min_Int) | \
            (dati1v.longitude[k] > Lon_Max_Int) | \
            (dati1v.latitude[k] < Lat_Min_Int) | \
            (dati1v.latitude[k] > Lat_Max_Int)):
        # print('trovato')
        Fl_Viag[k] = 13
    else:
        Fl_Viag[k] = 12

    k = len(dati1v) - 1  # ______________________________ Ultima registrazione
    if ((dati1v.longitude[k] < Lon_Min_Int) | \
            (dati1v.longitude[k] > Lon_Max_Int) | \
            (dati1v.latitude[k] < Lat_Min_Int) | \
            (dati1v.latitude[k] > Lat_Max_Int)):
        Fl_Viag[k] = 23
    else:
        Fl_Viag[k] = 22

    # _______________________________________________Inizio NOV. e Fine MAR.
    if sum(dati1v.timedate > '2019-04-01') > 1:
        k = dati1v[dati1v.timedate > '2019-04-01'].index[0]
        if k > 0:  # Assicura che MARZO è presente
            if ((dati1v.longitude[k] < Lon_Min_Int) | \
                    (dati1v.longitude[k] > Lon_Max_Int) | \
                    (dati1v.latitude[k] < Lat_Min_Int) | \
                    (dati1v.latitude[k] > Lat_Max_Int)):
                Fl_Viag[k] = 13
                Fl_Viag[k - 1] = 23
            else:
                Fl_Viag[k] = 12
                Fl_Viag[k - 1] = 22

    # ==================================================================
    # ______ calcola:   INIZIO-FINE     da CONFINE  ____________________
    # ==================================================================
    for k in range(len(dati1v)):
        if ((dati1v.longitude[k] < Lon_Min_Int) | \
            (dati1v.longitude[k] > Lon_Max_Int) | \
            (dati1v.latitude[k] < Lat_Min_Int) | \
            (dati1v.latitude[k] > Lat_Max_Int)) & \
                (dati1v.Delta_sec[k] > D_sec):
            D_Distance = (dati1v.progressive[k] - dati1v.progressive[k - 1]) / 1000
            if D_Distance > 0.5:
                # print(k, ' --  ',D_Distance)
                # ..........................
                if (Fl_Viag[k - 1] == 0) & (Fl_Viag[k] == 0):
                    # print('{0:5.0f}--- {1:9.6f} - {2:9.6f}  '.format(k, dati1v.longitude[k], dati1v.latitude[k]), end='\n' )
                    Fl_Viag[k] = 11
                    Fl_Viag[k - 1] = 21

    # ==================================================================
    # ______ calcola:   INIZIO-FINE     REGOLARI   ____________________
    # ==================================================================
    Indir_Ini_Viaggi_Regol = dati1v.index[(dati1v.Delta_sec > D_sec) & (Fl_Viag == 0)]
    if len(Indir_Ini_Viaggi_Regol) > 0:
        Fl_Viag[Indir_Ini_Viaggi_Regol] = 10

    # ___________________Vanno settatti a 20 solo i FLAG precedenti == 0
    # ___________________Se i FlAG precedenti sono = 10,11,12,13 va
    # ___________________AZZERATO il flag attuale  ==10
    for k in Indir_Ini_Viaggi_Regol:
        if (Fl_Viag[k - 1] > 9) & (Fl_Viag[k - 1] < 19):
            # print(k,'='*50, '  ', Fl_Viag[k], Fl_Viag[k-1] )
            Fl_Viag[k] = 0
        else:
            if Fl_Viag[k - 1] == 0:   Fl_Viag[k - 1] = 20

    # ==================================================================
    #   Puntatori di INIZIO e FINE  VIAGGIO
    # ==================================================================

    Pun_Ini = dati1v.index[(Fl_Viag > 9) & (Fl_Viag < 20)]
    Pun_Fin = dati1v.index[Fl_Viag > 19]

    #  ERRORE se:  Puntatori Inizio e Fine hanno differenti dimensioni
    if len(Pun_Ini) != len(Pun_Fin):
        print('=' * 40, ' ERRORE ', '=' * 40)
        print('\n len(Pun_Ini) =', len(Pun_Ini), 'len(Pun_Fin) =', len(Pun_Fin), '\n', '=' * 89)
        print('Pun_Ini = ', Pun_Ini)
        print('Pun_Fin = ', Pun_Fin)
        # stop

        _Veic_da_Eliminare = 1  # 1: Puntatori partenze (Pun_Ini) ha diversa lunghezza
        # rispetto a quelo degli arrivi (Pun_Fin)

    if _Veic_da_Eliminare == 0:
        # _________ Toglie KM per FCeFT  (Fuori Confine e FuoriTempo )
        #                                  Azzera i km per chi rientra:
        #                                  dal confine o rientra a Novembre
        indirizzi = dati1v.index[Pun_Ini][Fl_Viag[Pun_Ini] != 10]
        # print( dati1v.loc[ indirizzi, 'Km'] )
        dati1v.loc[indirizzi, 'Km'] = 0

        # ==============================================================================
        #   Corregge viaggi di km < 0.01 , dovute a riaccensioni-spegnimenti
        #   durante una sosta lunga più di 24 ore
        #
        #   Il falso viaggio viene corretto con:  'Fl_Viag' = 5
        # ==============================================================================

        conta = 0
        # print(' Ind (PunIni PunFi)[Fl_Viag:In-Fi] Sosta    Km')
        for k in range(len(Pun_Ini)):
            km_Viag = sum(dati1v.Km[Pun_Ini[k] + 1:Pun_Fin[k] + 1])
            if km_Viag < 0.01:
                conta = conta + 1
                # durata_viaggio = (dati1v.timedate[Pun_Fin[k]] - dati1v.timedate[ Pun_Ini[k] ]).value/1e9/3600  #  Ore
                # print('{0:3.0f}° ({1:5.0f} {2:5.0f} ) '.format(conta, Pun_Ini[k], Pun_Fin[k]), end=' ' )
                # print('[ {0:3.0f}-{1:3.0f} ] '.format(Fl_Viag[ Pun_Ini[k]], Fl_Viag[ Pun_Fin[k]]), end=' '   )
                # print(' {0:5.2f} -{1:5.2f} '.format(  durata_viaggio, km_Viag) )
                # print('\n\n----------------  Da: ', Pun_Ini[k], '  A: ', Pun_Fin[k] )

                Fl_Viag[Pun_Ini[k]:Pun_Fin[k] + 1] = 5

        # ==============================================================================
        #   Corregge viaggi di durata eccessiva, dovute a riaccensioni-spegnimenti
        #   prima di una vera partenza che avviene oltre 2 ore dopo
        #
        #   Il falso viaggio viene corretto con:  'Fl_Viag' = 5
        # ==============================================================================

        Pun_Ini = dati1v.index[(Fl_Viag > 9) & (Fl_Viag < 20)]
        Pun_Fin = dati1v.index[Fl_Viag > 19]
        # ..........................
        # for kk in range(len(Pun_Ini)):
        #    print(' '*30, kk, Pun_Ini[kk], Pun_Fin[kk])
        # ..........................

        conta = 0
        # print(' Ind (PunIni PunFi)[Fl_Viag:In-Fi] Sosta    Km  New_Inizio')
        for k in range(len(Pun_Ini)):
            durata_viaggio = (dati1v.timedate[Pun_Fin[k]] - dati1v.timedate[Pun_Ini[k]]).value / 1e9 / 3600  # Ore
            if durata_viaggio > 2:
                conta = conta + 1
                Da = Pun_Ini[k]
                A = Pun_Fin[k]
                if sum(dati1v.Delta_sec.values[Da:A] > 2 * D_sec) > 0:  # Se: > 0  ci sono
                    # soste ANOMALE
                    New_Inizio = dati1v.Delta_sec[Da:A][dati1v.Delta_sec[Da:A] > 2 * D_sec].index[0]

                    # km_Viag = sum(dati1v.Km[ Pun_Ini[k]+1:Pun_Fin[k]+1 ])
                    # print('{0:3.0f}° ({1:5.0f} {2:5.0f} ) '.format(conta, Pun_Ini[k], Pun_Fin[k]), end=' ' )
                    # print('[ {0:3.0f}-{1:3.0f} ] '.format(Fl_Viag[ Pun_Ini[k]], Fl_Viag[ Pun_Fin[k]]), end=' '   )
                    # print(' {0:5.2f} -{1:5.2f} '.format(  durata_viaggio, km_Viag), end=' ' )
                    # print('({0:5.0f} ) '.format(New_Inizio) )
                    # print('----------------  Da: ', Da, '  Inizio: ', New_Inizio, '\n' )

                    Fl_Viag[Da:New_Inizio] = 5
                    if Fl_Viag[New_Inizio] == 0:    Fl_Viag[New_Inizio] = 10

    return (Fl_Viag, _Veic_da_Eliminare)


def Crea_Id_Viaggi(dati1v):
    # ==========================================================================
    # Crea il Vettore Id_Viaggi sul database "dati1v"
    #        questo vettore è composto da singole stinge stringhe contenenti
    #        l'ID del veicolo + _ + N°Viaggio
    #
    # INPUT:  nessuno
    # OUTPUT: vettore Id_Viaggi
    # ==========================================================================

    Id_Viaggi = ['xxx'] * len(dati1v)
    Id_Veic_str = str(dati1v.idterm[0]) + '_'

    Pun_Ini = dati1v.index[(dati1v.Fl_Viag > 9) & (dati1v.Fl_Viag < 20)]
    Pun_Fin = dati1v.index[dati1v.Fl_Viag > 19]

    for k in range(len(Pun_Ini)):
        In = Pun_Ini[k];
        Fi = Pun_Fin[k];
        Id_Viaggi[In:Fi + 1] = [Id_Veic_str + str(k)] * (Fi + 1 - In)

    return (Id_Viaggi)


def Tab_Sintetica(dati1v):
    # ==========================================================================
    # Tabella sintetica   per i flag viaggio   Fl_Viag
    # Esegue solo una trampa
    # ==========================================================================

    Pun_Ini = dati1v.index[(dati1v.Fl_Viag > 9) & (dati1v.Fl_Viag < 20)]
    Pun_Fin = dati1v.index[dati1v.Fl_Viag > 19]

    print('  N°   km    (Sos.Pri Sos.Dopo) in minuti.')
    for k in range(len(Pun_Ini)):
        km_Viag = sum(dati1v.Km[Pun_Ini[k] + 1:Pun_Fin[k] + 1])
        if k < (len(Pun_Ini) - 1):
            print(' {0:3.0f} {1:7.2f} ({2:7.2f}  {3:7.2f})'.format(k, km_Viag, \
                                                                   dati1v.Delta_sec[Pun_Ini[k]] / 60, \
                                                                   dati1v.Delta_sec[Pun_Fin[k] + 1] / 60), end='\n')
        else:
            print(' {0:3.0f} ({1:7.2f} {2:7.2f}     -   )'.format(k, sum(dati1v.Km[Pun_Ini[k] + 1:Pun_Fin[k] + 1]), \
                                                                  dati1v.Delta_sec[Pun_Ini[k]] / 60), end='\n')
    print('  N°   km    (Sos.Pri Sos.Dopo) in minuti.')

    return


def Tab_Completa(dati1v):
    # ==========================================================================
    # Tabella Completa   per i flag viaggio   Fl_Viag
    # Esegue solo una trampa
    # ==========================================================================

    Pun_Ini = dati1v.index[(dati1v.Fl_Viag > 9) & (dati1v.Fl_Viag < 20)]
    Pun_Fin = dati1v.index[dati1v.Fl_Viag > 19]

    print(
        '      Pun.Ini-Fin  [   km ](Fl_Viag) Data_Ini          Dat Fin          S.Prec  Viag.   S.Succ[Ore]  Id_Viag[In]   Id_Viag[Fi]')
    for k in range(len(Pun_Ini)):
        In = Pun_Ini[k];
        Fi = Pun_Fin[k];
        _km = (dati1v.Km_prog[Fi] - dati1v.Km_prog[In])
        _ODO = (dati1v.distance[Fi] - dati1v.distance[In]) / 1000

        print('{0:3.0f}°:'.format(k), end='')
        print('{0:6.0f} -{1:6.0f}  '.format(In, Fi), end='')
        print('[{0:5.1f} ]'.format(_km), end='')
        print('({0:2.0f}-{1:2.0f} )  '.format(Fl_Viag[In], Fl_Viag[Fi]), end='')
        print(str(dati1v.timedate[In])[5:], '  ', str(dati1v.timedate[Fi])[5:], end='')
        # print('  {0:8.0f}'.format( dati1v.Delta_sec[In]  ), end='')
        # print(' +   {0:5.0f}'.format( int((dati1v.timedate[Fi]  - dati1v.timedate[In]).value/1e9 )), end='')

        print(' {0:8.3f}'.format(dati1v.Delta_sec[In] / 3600), end='')
        print(' {0:6.3f}'.format((dati1v.timedate[Fi] - dati1v.timedate[In]).value / 1e9 / 3600), end='')
        if k < len(Pun_Ini) - 1:
            # print('  {0:7.3f}'.format( dati1v.Delta_sec[Fi+1]/3600  ), end='\n')
            print('  {0:7.3f}'.format(dati1v.Delta_sec[Fi + 1] / 3600), end='    ')
            print(' {0:13s}'.format(dati1v.Id_Viaggi[In]), end='')
            print('  {0:13s}'.format(dati1v.Id_Viaggi[Fi]), end='\n')
    # print('\n        Pun.Ini-Fin  [   km ](Fl_Viag) Data_Ini          Dat Fin          S.Prec  Viag.   S.Succ [Ore]' )
    print(
        '\n        Pun.Ini-Fin  [   km ](Fl_Viag) Data_Ini          Dat Fin          S.Prec  Viag.   S.Succ[Ore]  Id_Viag[In]   Id_Viag[Fi]')

    return


def Setta_Soste_Generiche_Notte_e_Giorno(_Fl_Stampa):
    # ==========================================================================
    #  Definisce TUTTE le soste come generiche in 4 sole categorie:
    #                   Soste Notturna FINE   (TipoSosta=220)
    #                   Soste    "     INIZIO (TipoSosta=120)
    #                   Soste Giornaliera FINE (TipoSosta=210)
    #                   Soste    "        INIZIO (TipoSosta=110)
    # Successivamente altre routine le SPECIFICHERANNO meglio secondo
    # l'elenco completo qui di seguito:
    #
    #  _Fl_Stampa:  = True  --> stampa Diagnostiche
    #            = False --> non le stampa
    #
    # ==========================================================================
    #   CODICE    TIPOSOSTA
    #
    #      0	     In viaggio
    #      5      Sosta Anomala. Dovuta ad accens-spegnimenti consecutivi a km=0, ma Dt>>1 ora
    #    110	     Inizio Sosta giorno generica
    #    111	     Inizio      "        "       prima casa
    #    112	     Inizio      "        "       seconda casa
    #    115	     Inizio      "        "       1° Lavoro
    #    116	     Inizio      "        "       2° Lavoro
    #    117	     Inizio      "        "       CONFINE
    #    120	     Inizio Sosta Notte generica
    #    121	     Inizio       "       "      prima casa
    #    122	     Inizio       "       "      seconda casa
    #    127	     Inizio      "       "        CONFINE
    #    210	     Fine        "     giorno  generica
    #    211	     Fine        "        "       prima casa
    #    212	     Fine        "        "       seconda casa
    #    215	     Fine        "        "       1° Lavoro
    #    216	     Fine        "        "       2° Lavoro
    #    217	     Fine         "       "        CONFINE
    #    220	     Fine         "     Notte generica
    #    221	     Fine         "       "        prima casa
    #    222	     Fine         "       "        seconda casa
    #    227	     Fine         "       "        CONFINE

    # ==========================================================================
    # Variab.Ingresso:  _Fl:Diag     = flag diagnostiche (= True: le stampa)
    #                                                    (= False: non stampa)
    # Variab.Uscita:    TipoSosta    = vettore contenente codice TipoSosta
    #                                  per tutte le registrazioni del veicolo
    #                   pun_Notti_Strane_D = viaggia durante la notte (punt. Destinazione)
    #                   pun_Notti_Strane_O = come sopra  (punt. Origine)
    # ==========================================================================
    Pun_Ini = dati1v.index[(dati1v.Fl_Viag > 9) & (dati1v.Fl_Viag < 20)]
    Pun_Fin = dati1v.index[dati1v.Fl_Viag > 19]

    TipoSosta = np.zeros(len(dati1v), dtype=int)  # Tipo Sosta

    if _Fl_Stampa: print('      Inizio Sosta    Fine Sosta        OreDurataSosta')

    N_Notti = 0
    N_Notti_Strane = 0  # NOTTI STRANE: verifica la presenza di strani
    # Viaggi> 4 ore (problema dovuto ad accensioni e
    # spegnimenti senza fare km)
    # Non dovrebbe essere più presente.
    # Si è posto TipoSosta=5  alle Accensioni/Spegnimenti

    pun_Notti_Strane_D = np.zeros(len(Pun_Fin), dtype='int')  # Notti Strane DESTINAZIONE
    pun_Notti_Strane_O = np.zeros(len(Pun_Fin), dtype='int')  # Notti Strane ORIGINE
    for k in range(len(Pun_Fin) - 1):
        Ora_Decim_Fine_Viag = dati1v.timedate[Pun_Fin[k]].hour + \
                              dati1v.timedate[Pun_Fin[k]].minute / 60 + \
                              dati1v.timedate[Pun_Fin[k]].second / 3600

        Sosta_Succes = sum(dati1v.Delta_sec[Pun_Fin[k]:Pun_Ini[k + 1] + 1])

        if _Fl_Stampa:
            print('{0:3.0f}°: '.format(k), end='')
            print(str(dati1v.timedate[Pun_Fin[k]])[5:], end='  ')
            print(str(dati1v.timedate[Pun_Ini[k + 1]])[5:], end='  ')
            print('{0:7.2f}: '.format(Sosta_Succes / 3600), end='')

        #                     E'sosta NOTTURNA se 2 ore dopo la mezzanotte è a casa
        if (Ora_Decim_Fine_Viag + Sosta_Succes / 3600) > (24 + 2):
            TipoSosta[Pun_Fin[k]] = 220
            TipoSosta[Pun_Ini[k + 1]] = 120
            N_Notti = N_Notti + 1
            if _Fl_Stampa:  print('NOTTE ', N_Notti, '\n')

        else:
            durata_viaggio_succes = (dati1v.timedate[Pun_Fin[k + 1]] - dati1v.timedate[
                Pun_Ini[k + 1]]).value / 1e9 / 3600  # Ore
            km_viaggio_succes = (dati1v.Km_prog[Pun_Fin[k + 1]] - dati1v.Km_prog[Pun_Ini[k + 1]])  # km

            if (durata_viaggio_succes < 4) | (km_viaggio_succes > 60):
                TipoSosta[Pun_Fin[k]] = 210
                TipoSosta[Pun_Ini[k + 1]] = 110
                if _Fl_Stampa:  print('       GIORNO')
            else:  # _______ Notti Strane
                TipoSosta[Pun_Fin[k]] = 220
                TipoSosta[Pun_Ini[k + 1]] = 120
                pun_Notti_Strane_D[N_Notti_Strane] = Pun_Fin[k]
                pun_Notti_Strane_O[N_Notti_Strane] = Pun_Ini[k + 1]
                N_Notti = N_Notti + 1
                N_Notti_Strane = N_Notti_Strane + 1

                if _Fl_Stampa:  print('NOTTE ', N_Notti, '(di cui ', N_Notti_Strane, ' strane  ***\n', ' ' * 60, '....')
                # print( '-'*30,'ora =', Ora_Decim_Fine_Viag, ' + ',dati1v.Delta_sec[Pun_Fin[k]+1]/3600, 'vvv=', durata_viaggio_succes  )
    if _Fl_Stampa: print('      Inizio Sosta    Fine Sosta        OreDurataSosta\n')

    pun_Notti_Strane_D = pun_Notti_Strane_D[:N_Notti_Strane]
    pun_Notti_Strane_O = pun_Notti_Strane_O[:N_Notti_Strane]

    return (TipoSosta, pun_Notti_Strane_D, pun_Notti_Strane_O)


def Calcola_Coord_Case():
    # ==========================================================================
    #
    #    Calcolo  coord. PRIMA e SECONDA CASA
    #
    #    "Matr_Cluster" = Matrice quadrata di dimensioni = al N° di soste notte
    #    Nella 1° colonna mette un 1 a tutte le soste vicine alla sosta N° 1
    #    Nella 2° colonna mette un 1 a tutte le soste vicine alla sosta N° 2
    #    .......
    #    La colonna con più "1" indica  tutte le soste della PRIMA CASA.
    #    Se nella "Matr_Cluster" si azzerano le soste della prima casa, si può
    #    ricercare la seconda sosta più frequente == SECONDA CASA
    # ==========================================================================
    # Variab.Uscita:   Coord_1_Casa  = Longit. e Latitud. 1° casa
    #                  Coord_2_Casa  = Longit. e Latitud. 2° casa
    #                                          Sono = 0 se non si trovano
    #                  Numero_Case        = N° di case trovate: 0, 1, 2
    #
    # Es. chiamata:    Coord_1_Casa, Coord_2_Cas, N_Case = Calcola_Coord_Case()
    # ==========================================================================
    Coord_1_Casa = [-1, -1]
    Coord_2_Casa = [-1, -1]
    Numero_Case = 0

    pun_Soste_Notte = dati1v.index[(dati1v.TipoSosta == 220) & (dati1v.Fl_Viag != 21)].values
    Matr_Cluster = np.zeros((len(pun_Soste_Notte), len(pun_Soste_Notte)), dtype=int)

    for k in range(len(pun_Soste_Notte)):
        Lon = dati1v.longitude[pun_Soste_Notte[k]]  # Coord. sosta
        Lat = dati1v.latitude[pun_Soste_Notte[k]]

        LoMin = Lon - D_lon;
        LoMax = Lon + D_lon  # Coor. RIQUADRO
        LaMin = Lat - D_lat;
        LaMax = Lat + D_lat

        LoX = dati1v.longitude[pun_Soste_Notte]  # Coord. di tutte  le soste
        LaX = dati1v.latitude[pun_Soste_Notte]
        # Punt.Soste interne alRIQUADRO
        pun = (LoX > LoMin) & (LoX < LoMax) & \
              (LaX > LaMin) & (LaX < LaMax)
        Matr_Cluster[:, k] = pun * 1

    Massimi = sum(Matr_Cluster)

    if len(Matr_Cluster) > 0:
        if max(Massimi) < 2:

            Coord_1_Casa = [0, 0]
            Coord_2_Casa = [0, 0]
        else:
            Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
            Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

            Coord_1_Casa = \
                [round(np.mean(dati1v.longitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6), \
                 round(np.mean(dati1v.latitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6)]

            Numero_Case = 1

            # ______________________________________________________________________________
            #
            #       TROVA SECONDA CASA
            #
            # Toglie da 'Matr_Cluster' i veic della prima casa e cerca una seconda casa
            # ______________________________________________________________________________

            for k in range(len(pun_Soste_Notte)):
                if Tutte_le_Soste_del_Cluster[k]:
                    Matr_Cluster[k, :] = 0

            Massimi = sum(Matr_Cluster)
            if max(Massimi) < 2:

                Coord_2_Casa = [0, 0]
            else:
                Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
                Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

                Coord_2_Casa = \
                    [round(np.mean(dati1v.longitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6), \
                     round(np.mean(dati1v.latitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6)]
                Numero_Case = 2

    return (Coord_1_Casa, Coord_2_Casa, Numero_Case)


def Calcola_Coord_Lavori(_Sosta_Lav_Min):
    # ==========================================================================
    #
    #    T R O V A  coordinate   PRIMO e SECONDO LAVORO
    #
    #    "Matr_Cluster" = Matrice quadrata di dimensioni = al N° di soste notte
    #    Nella 1° colonna mette un 1 a tutte le soste vicine alla sosta N° 1
    #    Nella 2° colonna mette un 1 a tutte le soste vicine alla sosta N° 2
    #    .......
    #    La colonna con più "1" indica  tutte le soste del PRIMO LAVORO.
    #    Se nella "Matr_Cluster" si azzerano le soste del primo lavoro, si può
    #    ricercare la seconda sosta più frequente = SECONDO LAVORO
    # ==========================================================================
    #
    # Variab.Uscita:   Coord_1_Lavoro  = Longit. e Latitud. 1° Lavoro
    #                  Coord_2_Lavoro  = Longit. e Latitud. 2° Lavoro
    #                                          Sono = 0 se non si trovano
    #                  Numero_Lavori        = N° di Lavori trovate: 0, 1, 2
    #
    # Es. chiamata:    Coord_1_Lavoro, Coord_2_Lavoro, N_Lavori = Calcola_Coord_Lavori(Sosta_Lav_Min)
    # ==========================================================================

    # print('CORREGGERE SOSTA_LAVORO_MINIMA', '*'*50)
    # print( '*'*50, ' Ora è :', _Sosta_Lav_Min, 'ore')
    Coord_1_Lavoro = [-1, -1]
    Coord_2_Lavoro = [-1, -1]
    Numero_Lavori = 0

    pun_Soste_Giorno = dati1v.index[(dati1v.TipoSosta == 210) & \
                                    (dati1v.Delta_sec > _Sosta_Lav_Min * 3600)].values

    Matr_Cluster = np.zeros((len(pun_Soste_Giorno), len(pun_Soste_Giorno)), dtype=int)

    for k in range(len(pun_Soste_Giorno)):
        Lon = dati1v.longitude[pun_Soste_Giorno[k]]  # Coord. sosta
        Lat = dati1v.latitude[pun_Soste_Giorno[k]]

        LoMin = Lon - D_lon;
        LoMax = Lon + D_lon  # Coor. RIQUADRO
        LaMin = Lat - D_lat;
        LaMax = Lat + D_lat

        LoX = dati1v.longitude[pun_Soste_Giorno]  # Coord. di tutte  le soste
        LaX = dati1v.latitude[pun_Soste_Giorno]
        # Punt.Soste interne alRIQUADRO
        pun = (LoX > LoMin) & (LoX < LoMax) & \
              (LaX > LaMin) & (LaX < LaMax)
        Matr_Cluster[:, k] = pun * 1

    Massimi = sum(Matr_Cluster)

    if len(Matr_Cluster) > 0:
        if max(Massimi) < 2:

            Coord_1_Lavoro = [0, 0]
            Coord_2_Lavoro = [0, 0]
        else:
            Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
            Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

            Coord_1_Lavoro = \
                [round(np.mean(dati1v.longitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6), \
                 round(np.mean(dati1v.latitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6)]

            Numero_Lavori = 1

            # ______________________________________________________________________________
            #
            #       TROVA SECONDO LAVORO
            #
            # Toglie da 'Matr_Cluster' i veic del primo lavoro e cerca il secondo lavoro
            # ______________________________________________________________________________

            for k in range(len(pun_Soste_Giorno)):
                if Tutte_le_Soste_del_Cluster[k]:
                    Matr_Cluster[k, :] = 0

                Massimi = sum(Matr_Cluster)
                if max(Massimi) < 2:

                    Coord_2_Lavoro = [0, 0]
                else:
                    Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
                    Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

                    Coord_2_Lavoro = \
                        [round(np.mean(dati1v.longitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6), \
                         round(np.mean(dati1v.latitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6)]
                    Numero_Lavori = 2

    return (Coord_1_Lavoro, Coord_2_Lavoro, Numero_Lavori)


def Setta_Soste_Specifica(Fine_Sosta_Generica, \
                          Fine_Sosta_Specifica, Inizio_Sosta_Specifica,
                          Coord):
    # ==========================================================================
    #   Cambia la sosta Generica in "dati1v.TipoSosta" (220 notte o 210 Giorno)
    #   con quella specifica fornita.
    #   (NOTA: per la sosta GENERICA è richiesta solo quella di FINE (quella di
    #          INIZIO è consecutiva), mentre  quella SPECIFICA è fornita sia
    #          quella di FINE e quella di INIZIO)
    # ==========================================================================
    #   INPUT:   Fine_Sosta_Generica   = codice sosta generica da sotituire
    #            Fine_Sosta:specifica  = Codice fornito che sostituirà la sostagenerica
    #            Inizio_Sosta_Specifica= Setta con questo codice il valore "precedente" del fine Sosta
    # ==========================================================================
    #   CHIAMATA:   Setta_Soste_Specifica(Codice_Fine_Sosta_Generica,
    #                   Codice_Fine_Sosta_Specifica, Codice_Inizio_Sosta_Specifica,
    #                   Coordinate_luogo)
    #
    #     es.:      Setta_Soste_Specifica( 220, 221, 121, Coord_1_Casa)
    # ==========================================================================

    pun_Soste_Generica = dati1v.index[dati1v.TipoSosta == Fine_Sosta_Generica].values

    LoMin = Coord[0] - D_lon;
    LoMax = Coord[0] + D_lon
    LaMin = Coord[1] - D_lat;
    LaMax = Coord[1] + D_lat

    LoX = dati1v.longitude[pun_Soste_Generica]  # Coord. di tutte  le soste GENERICHE
    LaX = dati1v.latitude[pun_Soste_Generica]
    # Pun = True  se 1a Casa
    pun = (LoX > LoMin) & (LoX < LoMax) & \
          (LaX > LaMin) & (LaX < LaMax)
    # Elimina puntatori che non sono 1a Casa
    pun_Soste_Specifica = pun_Soste_Generica
    for k in range(len(pun_Soste_Generica) - 1, -1, -1):
        if pun[k:k + 1].values == False:
            pun_Soste_Specifica = np.delete(pun_Soste_Specifica, k)

    dati1v.loc[pun_Soste_Specifica, 'TipoSosta'] = Fine_Sosta_Specifica  # FINE Sosta
    # setta INIZIO Sosta alla registraz.successiva
    # Saltando le Soste Anomale (quelle = 5)
    for k in range(len(pun_Soste_Specifica)):
        pun = pun_Soste_Specifica[k] + 1  # punt.successivo a fine sosta
        while dati1v.TipoSosta[pun] == 5:
            pun = pun + 1
        dati1v.loc[pun, 'TipoSosta'] = Inizio_Sosta_Specifica
    return ()


def Calcola_Coord_Case():
    # ==========================================================================
    #
    #    Calcolo  coord. PRIMA e SECONDA CASA
    #
    #    "Matr_Cluster" = Matrice quadrata di dimensioni = al N° di soste notte
    #    Nella 1° colonna mette un 1 a tutte le soste vicine alla sosta N° 1
    #    Nella 2° colonna mette un 1 a tutte le soste vicine alla sosta N° 2
    #    .......
    #    La colonna con più "1" indica  tutte le soste della PRIMA CASA.
    #    Se nella "Matr_Cluster" si azzerano le soste della prima casa, si può
    #    ricercare la seconda sosta più frequente == SECONDA CASA
    # ==========================================================================
    # Variab.Uscita:   Coord_1_Casa  = Longit. e Latitud. 1° casa
    #                  Coord_2_Casa  = Longit. e Latitud. 2° casa
    #                                          Sono = 0 se non si trovano
    #                  Numero_Case        = N° di case trovate: 0, 1, 2
    #
    # Es. chiamata:    Coord_1_Casa, Coord_2_Cas, N_Case = Calcola_Coord_Case()
    # ==========================================================================
    Coord_1_Casa = [-1, -1]
    Coord_2_Casa = [-1, -1]
    Numero_Case = 0

    pun_Soste_Notte = dati1v.index[(dati1v.TipoSosta == 220) & (dati1v.Fl_Viag != 21)].values
    Matr_Cluster = np.zeros((len(pun_Soste_Notte), len(pun_Soste_Notte)), dtype=int)

    for k in range(len(pun_Soste_Notte)):
        Lon = dati1v.longitude[pun_Soste_Notte[k]]  # Coord. sosta
        Lat = dati1v.latitude[pun_Soste_Notte[k]]

        LoMin = Lon - D_lon;
        LoMax = Lon + D_lon  # Coor. RIQUADRO
        LaMin = Lat - D_lat;
        LaMax = Lat + D_lat

        LoX = dati1v.longitude[pun_Soste_Notte]  # Coord. di tutte  le soste
        LaX = dati1v.latitude[pun_Soste_Notte]
        # Punt.Soste interne alRIQUADRO
        pun = (LoX > LoMin) & (LoX < LoMax) & \
              (LaX > LaMin) & (LaX < LaMax)
        Matr_Cluster[:, k] = pun * 1

    Massimi = sum(Matr_Cluster)

    if len(Matr_Cluster) > 0:
        if max(Massimi) < 2:

            Coord_1_Casa = [0, 0]
            Coord_2_Casa = [0, 0]
        else:
            Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
            Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

            Coord_1_Casa = \
                [round(np.mean(dati1v.longitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6), \
                 round(np.mean(dati1v.latitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6)]

            Numero_Case = 1

            # ______________________________________________________________________________
            #
            #       TROVA SECONDA CASA
            #
            # Toglie da 'Matr_Cluster' i veic della prima casa e cerca una seconda casa
            # ______________________________________________________________________________

            for k in range(len(pun_Soste_Notte)):
                if Tutte_le_Soste_del_Cluster[k]:
                    Matr_Cluster[k, :] = 0

            Massimi = sum(Matr_Cluster)
            if max(Massimi) < 2:

                Coord_2_Casa = [0, 0]
            else:
                Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
                Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

                Coord_2_Casa = \
                    [round(np.mean(dati1v.longitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6), \
                     round(np.mean(dati1v.latitude[pun_Soste_Notte[Tutte_le_Soste_del_Cluster]]), 6)]
                Numero_Case = 2

    return (Coord_1_Casa, Coord_2_Casa, Numero_Case)


def Calcola_Coord_Lavori(_Sosta_Lav_Min):
    # ==========================================================================
    #
    #    T R O V A  coordinate   PRIMO e SECONDO LAVORO
    #
    #    "Matr_Cluster" = Matrice quadrata di dimensioni = al N° di soste notte
    #    Nella 1° colonna mette un 1 a tutte le soste vicine alla sosta N° 1
    #    Nella 2° colonna mette un 1 a tutte le soste vicine alla sosta N° 2
    #    .......
    #    La colonna con più "1" indica  tutte le soste del PRIMO LAVORO.
    #    Se nella "Matr_Cluster" si azzerano le soste del primo lavoro, si può
    #    ricercare la seconda sosta più frequente = SECONDO LAVORO
    # ==========================================================================
    #
    # Variab.Uscita:   Coord_1_Lavoro  = Longit. e Latitud. 1° Lavoro
    #                  Coord_2_Lavoro  = Longit. e Latitud. 2° Lavoro
    #                                          Sono = 0 se non si trovano
    #                  Numero_Lavori        = N° di Lavori trovate: 0, 1, 2
    #
    # Es. chiamata:    Coord_1_Lavoro, Coord_2_Lavoro, N_Lavori = Calcola_Coord_Lavori(Sosta_Lav_Min)
    # ==========================================================================

    # print('CORREGGERE SOSTA_LAVORO_MINIMA', '*'*50)
    # print( '*'*50, ' Ora è :', _Sosta_Lav_Min, 'ore')
    Coord_1_Lavoro = [-1, -1]
    Coord_2_Lavoro = [-1, -1]
    Numero_Lavori = 0

    pun_Soste_Giorno = dati1v.index[(dati1v.TipoSosta == 210) & \
                                    (dati1v.Delta_sec > _Sosta_Lav_Min * 3600)].values

    Matr_Cluster = np.zeros((len(pun_Soste_Giorno), len(pun_Soste_Giorno)), dtype=int)

    for k in range(len(pun_Soste_Giorno)):
        Lon = dati1v.longitude[pun_Soste_Giorno[k]]  # Coord. sosta
        Lat = dati1v.latitude[pun_Soste_Giorno[k]]

        LoMin = Lon - D_lon;
        LoMax = Lon + D_lon  # Coor. RIQUADRO
        LaMin = Lat - D_lat;
        LaMax = Lat + D_lat

        LoX = dati1v.longitude[pun_Soste_Giorno]  # Coord. di tutte  le soste
        LaX = dati1v.latitude[pun_Soste_Giorno]
        # Punt.Soste interne alRIQUADRO
        pun = (LoX > LoMin) & (LoX < LoMax) & \
              (LaX > LaMin) & (LaX < LaMax)
        Matr_Cluster[:, k] = pun * 1

    Massimi = sum(Matr_Cluster)

    if len(Matr_Cluster) > 0:
        if max(Massimi) < 2:

            Coord_1_Lavoro = [0, 0]
            Coord_2_Lavoro = [0, 0]
        else:
            Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
            Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

            Coord_1_Lavoro = \
                [round(np.mean(dati1v.longitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6), \
                 round(np.mean(dati1v.latitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6)]

            Numero_Lavori = 1

            # ______________________________________________________________________________
            #
            #       TROVA SECONDO LAVORO
            #
            # Toglie da 'Matr_Cluster' i veic del primo lavoro e cerca il secondo lavoro
            # ______________________________________________________________________________

            for k in range(len(pun_Soste_Giorno)):
                if Tutte_le_Soste_del_Cluster[k]:
                    Matr_Cluster[k, :] = 0

                Massimi = sum(Matr_Cluster)
                if max(Massimi) < 2:

                    Coord_2_Lavoro = [0, 0]
                else:
                    Sosta_con_Cluster_piu_grande = np.arange(len(Massimi))[Massimi == max(Massimi)][0]
                    Tutte_le_Soste_del_Cluster = Matr_Cluster[:, Sosta_con_Cluster_piu_grande] == 1

                    Coord_2_Lavoro = \
                        [round(np.mean(dati1v.longitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6), \
                         round(np.mean(dati1v.latitude[pun_Soste_Giorno[Tutte_le_Soste_del_Cluster]]), 6)]
                    Numero_Lavori = 2

    return (Coord_1_Lavoro, Coord_2_Lavoro, Numero_Lavori)


def Trova_Luogo(Coord):
    # ==========================================================================
    #   Trova le soste interne ad un riquadro di 1 km di lato e centrato
    #   sulle coordinate fornite.
    #   RITORNA un vettore lungo come il veicolo (dati1v) contenente 1 se la
    #   sosta cade nel riquadro, diversamente contiene 0.
    # ==========================================================================
    #
    #   CHIAMATA:   Univ = Trova_Luogo( Coordinate_luogo_Da_Trovare )
    #
    #               Univ: è il vettore che ritorna dalla routine
    # ==========================================================================

    D_lon = 0.0065;
    D_lat = 0.0045  # Delta gradi, corrispondenti a 500 m.

    Lon_Min_Univ = Coord[0] - D_lon;
    Lon_Max_Univ = Coord[0] + D_lon
    Lat_Min_Univ = Coord[1] - D_lat;
    Lat_Max_Univ = Coord[1] + D_lat

    pun = (dati1v.longitude > Lon_Min_Univ) & (dati1v.longitude < Lon_Max_Univ) & \
          (dati1v.latitude > Lat_Min_Univ) & (dati1v.latitude < Lat_Max_Univ) & \
          (dati1v.TipoSosta > 0)

    Sosta_alle_coord_fornite = np.zeros(len(pun), dtype=int)
    Sosta_alle_coord_fornite[pun * range(len(pun))] = 1
    # Saltando le Soste Anomale (quelle = 5)
    return (Sosta_alle_coord_fornite)


def Plot_mappa(Nfig, Titolo, Puntatore, Simbolo, Colore):
    # ==========================================================================
    #   Plot_mappa (Nfig, Titolo, Puntatore, Simbolo, Colore)
    #
    #   Crea: le strade di sottofondo, il tracciato del veicolo in giallo,
    #         le posiz. 1a-2a Casa e lavoro, in più traccia i punti del puntatore
    #
    #    Nfig,       N° della Fig. Creata
    #    Titolo,     Titolo del plot
    #    Puntatore,  Punt. delle coordinate graficate
    #    Simbolo,    Simbolo del plot
    #    Colore      Colore del Simbolo
    # ==========================================================================

    fig__ = plt.figure(Nfig, figsize=(12, 10.7), dpi=80, facecolor='w')
    #                                       V_Batt
    ax__ = fig__.add_subplot(1, 1, 1)
    # _____________________________________SOTTOFONDO
    # ax__.plot( dati  .longitude, dati  .latitude, '.', color='yellow', markersize= 2 )
    # ax__.plot( dati  .longitude, dati  .latitude, '.', color='mistyrose', markersize= 2 )
    ax__.plot(dati.longitude, dati.latitude, '.', color='greenyellow', markersize=6)
    # ax__.plot( dati1v.longitude, dati1v.latitude, '.', color='cyan', markersize= 2 )
    ax__.set_xlabel('Longitude'), ax__.set_ylabel('Latitude')
    ax__.set_title(Titolo + '  ' + str(sum(Puntatore)) + ' soste trovate')

    # _____________________________________PLOT tutti i VIAGGI del Veicolo
    # ax__.plot( dati1v.longitude, dati1v.latitude, color='green', markersize= 4 )
    # ax__.plot( dati1v.longitude, dati1v.latitude, color='yellow', markersize= 4 )
    ax__.plot(dati1v.longitude, dati1v.latitude, '.', color='yellow', markersize=6)

    '''
    #____________________________  Riquadro Dichiarato
    ax__.plot(  [Lon_Min_Dic, Lon_Min_Dic, Lon_Max_Dic, Lon_Max_Dic, Lon_Min_Dic], [Lat_Min_Dic, Lat_Max_Dic, Lat_Max_Dic, Lat_Min_Dic, Lat_Min_Dic] )
    '''
    # ____________________________  Riquadro MISURATO
    ax__.plot([Lon_Min_Ris, Lon_Min_Ris, Lon_Max_Ris, Lon_Max_Ris, Lon_Min_Ris],
              [Lat_Min_Ris, Lat_Max_Ris, Lat_Max_Ris, Lat_Min_Ris, Lat_Min_Ris], 'm')
    # ____________________________  Riquadro INTERNO
    ax__.plot([Lon_Min_Int, Lon_Min_Int, Lon_Max_Int, Lon_Max_Int, Lon_Min_Int],
              [Lat_Min_Int, Lat_Max_Int, Lat_Max_Int, Lat_Min_Int, Lat_Min_Int], '.--g')

    # ____________________________  Riquadro UNIVERSITA
    ax__.plot([Lon_Min_Univ, Lon_Min_Univ, Lon_Max_Univ, Lon_Max_Univ, Lon_Min_Univ],
              [Lat_Min_Univ, Lat_Max_Univ, Lat_Max_Univ, Lat_Min_Univ, Lat_Min_Univ], '.--m', linewidth=4)

    # _________________________Plot   P O S I Z I O N E    C A S A  e  LAVORO
    if Coord_1_Casa[0] > 0:
        ax__.plot(Coord_1_Casa[0], Coord_1_Casa[1], '+b', markersize=18)
    if Coord_2_Casa[0] > 0:
        ax__.plot(Coord_2_Casa[0], Coord_2_Casa[1], 'xb', markersize=18)
    if Coord_1_Lavoro[0] > 0:
        ax__.plot(Coord_1_Lavoro[0], Coord_1_Lavoro[1], '+r', markersize=18)
    if Coord_2_Lavoro[0] > 0:
        ax__.plot(Coord_2_Lavoro[0], Coord_2_Lavoro[1], 'xr', markersize=18)

    print('Case:   1°  "+"    2° "x"    BLUE')
    print('Lavoro: 1°  "+"    2° "x"    RED')
    print('Il veicolo ha: ', N_Case, 'Case e  ', N_Lavori, 'Lavori\n')
    print(' ' * 20, sum(Puntatore), 'soste trovate')

    ax__.plot(dati1v.longitude[Puntatore], dati1v.latitude[Puntatore], Simbolo, color=Colore, markersize=8)

    return (ax__)


def Plot_viaggio(ax_, Da, A, Simbolo, Colore):
    # ==========================================================================
    # Da una finestra già esistente aggiunge il tracciato che va
    # dai puntatori:  Da, A
    # Il tracciato è fatto col Simbolo ed il Colore forniti
    #
    # es. Chiamata:
    # Plot_viaggio(ax50, 5358, 5396, '.', 'blue')
    # ==========================================================================
    ax_.plot(dati1v.longitude[Da:A + 1], dati1v.latitude[Da:A + 1], Simbolo, color=Colore, markersize=15)
    ax_.plot(dati1v.longitude[Da:A + 1], dati1v.latitude[Da:A + 1], '-', color=Colore)
    ax_.plot(dati1v.longitude[A], dati1v.latitude[A], '^r', markersize=10)
    ax_.plot(dati1v.longitude[Da], dati1v.latitude[Da], '.g', markersize=25)

    return


def Crea_df_Viaggi_Vuoto():
    # ==========================================================================
    # Crea il data-frame viaggi vuoto.
    # Quindi i viaggi di ogni veicolo verranno aggiunti di seguito
    # --------------------------------------------------------------------------
    # CHIAMATA:     df_Viag = Crea_df_Viaggi_Vuoto()
    # RITORNA:      data-frame viaggi VUOTO
    # ==========================================================================
    df = pd.DataFrame({'N_Viaggio': [],
                       'Pun_Ini': [],
                       'Pun_Fin': [],
                       'Id_Term': [],
                       'Veh_Type': [],

                       'O_TipoSosta': [],
                       'D_TipoSosta': [],
                       'O_Fl_Viag': [],
                       'D_Fl_Viag': [],
                       'O_Data': [],
                       'D_Data': [],
                       'O_long': [],
                       'D_long': [],
                       'O_lati': [],
                       'D_lati': [],
                       'O_Sosta_Univ': [],
                       'D_Sosta_Univ': [],
                       'O_Sosta_Dur': [],
                       'D_Sosta_Dur': [],

                       'Viag_Km': [],
                       'Viag_Durata': [],
                       'O_Day_of_year': [],
                       'O_Day_of_week': [],
                       'Id_Viaggi': [],
                       })
    df['N_Viaggio'] = df.N_Viaggio.astype('int32')
    df['Pun_Ini'] = df.Pun_Ini.astype('int32')
    df['Pun_Fin'] = df.Pun_Fin.astype('int32')
    df['Id_Term'] = df.Id_Term.astype('int32')
    df['Veh_Type'] = df.Veh_Type.astype('int32')
    df['O_TipoSosta'] = df.O_TipoSosta.astype('int32')
    df['D_TipoSosta'] = df.D_TipoSosta.astype('int32')
    df['O_Fl_Viag'] = df.O_Fl_Viag.astype('int32')
    df['D_Fl_Viag'] = df.D_Fl_Viag.astype('int32')
    df['O_Sosta_Univ'] = df.O_Sosta_Univ.astype('int32')
    df['D_Sosta_Univ'] = df.D_Sosta_Univ.astype('int32')
    df['O_Day_of_year'] = df.O_Day_of_year.astype('int32')
    df['O_Day_of_week'] = df.O_Day_of_week.astype('int32')

    return (df)


def Crea_df_Viaggi_1_Veicolo():
    # ==========================================================================
    # Crea il data-frame viaggi di un veicolo (dati1v).
    # --------------------------------------------------------------------------
    # CHIAMATA:     df_Viag_1v = Crea_df_Viaggi_1_Veicolo()
    # RITORNA:      data-frame viaggi di 1 veicolo
    # ==========================================================================
    Pun_Ini = dati1v.index[(Fl_Viag > 9) & (Fl_Viag < 20)]
    Pun_Fin = dati1v.index[Fl_Viag > 19]

    O_Sosta_Dur = np.array(dati1v.timedate[Pun_Ini[1:]].values - \
                           dati1v.timedate[Pun_Fin[:-1]].values, dtype=float) / 1e9 / 3600
    D_Sosta_Dur = np.append(O_Sosta_Dur, 0)
    O_Sosta_Dur = np.append(dati1v.Delta_sec[Pun_Ini[0]] / 3600, O_Sosta_Dur)

    Viag_Km = np.array(dati1v.Km_prog[Pun_Fin].values - dati1v.Km_prog[Pun_Ini].values)
    Viag_Durata = np.array(dati1v.timedate[Pun_Fin].values - \
                           dati1v.timedate[Pun_Ini].values, dtype=float) / 1e9 / 3600

    O_Day_of_week = np.zeros(len(Pun_Ini), dtype='int')
    for k in range(len(Pun_Ini) - 1):
        O_Day_of_week[k] = dati1v.timedate[Pun_Ini[k]].dayofweek

    df_1_veicolo = pd.DataFrame({'N_Viaggio': np.array(range(len(Pun_Ini))),
                                 'Pun_Ini': Pun_Ini,
                                 'Pun_Fin': Pun_Fin,
                                 'Id_Term': dati1v.idterm[Pun_Ini].values,
                                 'Id_Viaggi': dati1v.Id_Viaggi[Pun_Ini].values,
                                 'Veh_Type': dati1v.vehtype[Pun_Ini].values,

                                 'O_TipoSosta': dati1v.TipoSosta[Pun_Ini].values,
                                 'D_TipoSosta': dati1v.TipoSosta[Pun_Fin].values,
                                 'O_Fl_Viag': dati1v.Fl_Viag[Pun_Ini].values,
                                 'D_Fl_Viag': dati1v.Fl_Viag[Pun_Fin].values,
                                 'O_Data': dati1v.timedate[Pun_Ini].values,
                                 'D_Data': dati1v.timedate[Pun_Fin].values,
                                 'O_long': dati1v.longitude[Pun_Ini].values,
                                 'D_long': dati1v.longitude[Pun_Fin].values,
                                 'O_lati': dati1v.latitude[Pun_Ini].values,
                                 'D_lati': dati1v.latitude[Pun_Fin].values,
                                 'O_Sosta_Univ': dati1v.Univ[Pun_Ini].values,
                                 'D_Sosta_Univ': dati1v.Univ[Pun_Fin].values,
                                 'O_Sosta_Dur': O_Sosta_Dur,
                                 'D_Sosta_Dur': D_Sosta_Dur,

                                 'Viag_Km': Viag_Km,
                                 'Viag_Durata': Viag_Durata,
                                 'O_Day_of_year': dati1v.Day_of_year[Pun_Ini].values,
                                 'O_Day_of_week': O_Day_of_week,
                                 })

    return (df_1_veicolo)


def Crea_Vett_x_INFO_Veic():
    # ==========================================================================
    # Crea i singoli vettori VUOTI che sucessivamente saranno riempiti
    # e alla fine andranno comporre il data-frame: INFO VEICOLI
    # --------------------------------------------------------------------------
    # CHIAMATA:     _Id_Term,..., _Notti_Out = Crea_Vett_x_INFO_Veic()
    # RITORNA:      elenco vettori vuoti
    # ==========================================================================
    _Id_Term = []
    _Veh_Type = []
    _N_Giorni_Pres = []
    _Km_Percorsi = []
    _N_Case = []
    _N_Lavori = []
    _Coord_1_Casa = []
    _Coord_2_Casa = []
    _Coord_1_Lavoro = []
    _Coord_2_Lavoro = []
    _Notti_In = []
    _Notti_Out = []

    _Notti_1Casa = []
    _Notti_2Casa = []
    _Trans_Confine = []
    _N_Soste_Univ = []

    _Giorni_al_1Lav = []
    _Giorni_al_2Lav = []

    return (_Id_Term, _Veh_Type, _N_Giorni_Pres, _Km_Percorsi, \
            _N_Case, _N_Lavori, _Coord_1_Casa, _Coord_2_Casa, \
            _Coord_1_Lavoro, _Coord_2_Lavoro, _Notti_In, _Notti_Out, \
            _Notti_1Casa, _Notti_2Casa, _Trans_Confine, _N_Soste_Univ, \
            _Giorni_al_1Lav, _Giorni_al_2Lav)


def Aggiungi_1Veic_ai_Vett_di_INFO_Veic():
    # ==========================================================================
    # Aggiunge le INFO di un veicolo ("dati1v") ai vettori che comporranno
    # il futuro data-frame: INFO VEICOLI
    # --------------------------------------------------------------------------
    # CHIAMATA:     Aggiungi_1Veic_ai_Vett_x_INFO_Veic()
    # RITORNA:      Niente. I vettori li aggiorna direttamente lui
    # ==========================================================================

    _Id_Term.append(int(dati1v.idterm[:1]))
    _Veh_Type.append(int(dati1v.vehtype[:1]))
    _N_Giorni_Pres.append(N_Presenze_Giornaliere_Reali)
    _Km_Percorsi.append(float(dati1v.Km_prog[-1:]))  ### last row
    _N_Case.append(N_Case)
    _N_Lavori.append(N_Lavori)
    _Coord_1_Casa.append(Coord_1_Casa)
    _Coord_2_Casa.append(Coord_2_Casa)
    _Coord_1_Lavoro.append(Coord_1_Lavoro)
    _Coord_2_Lavoro.append(Coord_2_Lavoro)
    _Notti_In.append(Notti_In)
    _Notti_Out.append(Notti_Out)

    _Notti_1Casa.append(sum((dati1v.TipoSosta == 221)))
    _Notti_2Casa.append(sum((dati1v.TipoSosta == 222)))
    _Trans_Confine.append(sum((dati1v.TipoSosta == 117)))
    # _N_Soste_Univ   .append( int(sum(dati1v.Univ)/2+.5) )
    _N_Soste_Univ.append(sum(df_Viag_1v.O_Sosta_Univ))

    Giorni_al_1Lav = len(dati1v.Day_of_year[dati1v.TipoSosta == 115].value_counts())
    Giorni_al_2Lav = len(dati1v.Day_of_year[dati1v.TipoSosta == 116].value_counts())

    _Giorni_al_1Lav.append(Giorni_al_1Lav)
    _Giorni_al_2Lav.append(Giorni_al_2Lav)

    return


def Aggiungi_1Veic_Eliminato(Causa_eliminaz, _N_elim):
    # ==========================================================================
    # Aggiunge 1 veic. al numero di quelli eliminati
    # aggiornando i relativi vettori:
    #    Id_veic_elim    = Identificativo terminale
    #    Reg_veic_elim   = Né registrazioni del veicolo eliminato
    #    Causa_veic_elim = Codice causa eliminazione:
    #                      1 = diverso N° Punt. tra Part.e Arrivo
    #                      2 = N° viaggi < 3
    # --------------------------------------------------------------------------
    # CHIAMATA:     N_elim = Aggiungi_1Veic_Eliminato(Causa_eliminaz, N_elim)
    #                       Causa_eliminaz= Striga che descrive la causa dell'elimin.
    #                       N_elim        = N° dei veicoli eliminati
    # RITORNA:      _N_elim
    # ==========================================================================
    '''
    print('*' * 25, _N_elim, '°  Veicolo elimin.: ', dati1v.idterm[0], '  Scartato con codice:', Veic_da_Eliminare,
          '( ', Causa_eliminaz, ')')
    '''
    Id_veic_elim.append(dati1v.idterm[0])
    Reg_veic_elim.append(len(dati1v))
    Causa_veic_elim.append(Veic_da_Eliminare)
    _N_elim = _N_elim + 1

    return (_N_elim)


##############################################################################
##############################################################################
##############################################################################
########## -------------------------------------------------- ################
########## -------------------------------------------------- ################
########## -------------------------------------------------- ################
##############################################################################
##############################################################################

conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

#==============================================================================
#                                 I N I Z I O
#==============================================================================

D_sec = 300   # [sec].  SOSTA MINIMA CONSIDERATA
              # Con soste inferiori i viaggi sono CONCATENATI
              # USATO ANCHE per determinare FUORIUSCITA-INGRESSO dal riquadro

Sosta_Lav_Min = 2.0 # [ORE] Sosta Minima di lovoro (per identificarne il luogo)

Registr_Min = 50    # Al disotto di queste registrazioni il veicolo è scartato

Fl_Diag  = False    # = True --> stampa diagnostiche
#           Fl_Tab     stampa la tabella dei viaggi in fase iniziale
#                                    compresi ACCENSIONI-SPEGNIMENTI
Fl_Tab   = False    # = True --> stampa tabella preliminare e sintetica
#           Fl_Tab1   stampa la tabella dei viaggi CORRETTI in fase finale
Fl_Tab1  = False    # = True --> stampa tabella
Fl_Tempi = False    # = True --> stampa i tempi di esecuzione
Fl_Tab_Notte_Giorno = False  # = True --> Stampa soste NOTTE - GIORNO

if not(Fl_Diag):  print('Settare:  "Fl_Diag = True"  per Stampare le Diagnostiche')
if not(Fl_Tab):   print('Settare:  "Fl_Tab  = True"  per Stampare le Tabelle INIZIALI')
if not(Fl_Tab1):  print('Settare:  "Fl_Tab1 = True"  per Stampare le Tabelle FINALI CORRETTE')
if not(Fl_Tempi): print('Settare:  "Fl_Tempi= True"  per avere i tempi di esecuzione')

#______________________________ CREA data-Frame vuoto MA CON TUTTE LE VARIABILI
#                                 poi verranno aggiunti i veicoli, 1 alla volta
df_Viag = Crea_df_Viaggi_Vuoto()

#___________________________________________ Inizializza, come VUOTE, i vettori
#                                                    che comporranno: INFO_Veic

_Id_Term       , _Veh_Type      , _N_Giorni_Pres, _Km_Percorsi ,\
_N_Case        , _N_Lavori      , _Coord_1_Casa , _Coord_2_Casa,\
_Coord_1_Lavoro, _Coord_2_Lavoro, _Notti_In     , _Notti_Out   ,\
_Notti_1Casa   , _Notti_2Casa   , _Trans_Confine, _N_Soste_Univ ,\
_Giorni_al_1Lav, _Giorni_al_2Lav               = Crea_Vett_x_INFO_Veic()

#


M = 5   # Fattore Moltiplicativo (per variare il riquadro interno)
#D_lon = 0.00065; D_lat = 0.00045   # Delta gradi, corrispondenti a 50 m.
D_lon  = 0.0065;  D_lat = 0.0045    # Delta gradi, corrispondenti a 500 m.

#_______________________________________________Dichiarati da LORO
Lon_Min_Dic =  9.69;     Lon_Max_Dic = 11.30
Lat_Min_Dic = 45.13;     Lat_Max_Dic = 46.43
#________________________________________________Riscontrati
Lon_Min_Ris =  14.632569552675466;     Lon_Max_Ris = 15.387879548194734
Lat_Min_Ris =  37.19919030607089;     Lat_Max_Ris = 37.89601934429672
#________________________________________________Riquadro interno
Lon_Min_Int =  Lon_Min_Ris+D_lon*M;     Lon_Max_Int = Lon_Max_Ris-D_lon*M
Lat_Min_Int =  Lat_Min_Ris+D_lat*M;     Lat_Max_Int = Lat_Max_Ris-D_lat*M

M = 1;    #  M = 1.5
#________________________________________________Coord. UNIVERSITA'
Coord_Univ = [10.21759, 45.53764]
Parcheggio_Ingegneria = [10.23051, 45.56499]   ## parcheggio all'aperto
Lon_Univ     = Coord_Univ[0];       Lat_Univ     = Coord_Univ[1]
Lon_Min_Univ = Lon_Univ-D_lon*M;    Lon_Max_Univ = Lon_Univ+D_lon*M
Lat_Min_Univ = Lat_Univ-D_lat*M;    Lat_Max_Univ = Lat_Univ+D_lat*M


t0=tempo('INIZIO: ', 0)


# filed = 'C:\\Users\\asus pc\\.spyder-py3\\Mio_BRESCIA_dati\\Marzo_2019\\Dati_con_1576_Veic.csv'

#dati = pd.read_csv("pippo_11_2.csv", index_col=None)
# dati = pd.read_csv(filed, index_col=0)
#dati.to_csv(filed, sep=',', endcoding='utf-8')

'''# Per inserire le coordinate in metri prodotte dall'applicativo ReGeo
dati_coord = pd.read_csv(filed[:-4]+'_trasf.csv')
dati_coord.rename(columns={dati_coord.columns[0] : 'XUTM32', dati_coord.columns[1] : 'YUTM32' } , inplace=True)
dati['XUTM32'] = dati_coord.XUTM32
dati['YUTM32'] = dati_coord.YUTM32
del dati_coord
'''

tempo("==== Dopo aver caricato il file            ", t0)


if Fl_Tempi:   tempo("==== Dopo aver CARICATO il file          ", t0)

'''___________________________# ELENCO VARIABILI GIA' PRESENTI

'idrequest'  =  E' il cliente (ENEA) (sempre = 1)
'idterm'     =  Terminale (veicolo)
'timedate'   =  Data RTC
'latitude'   =  in direz. Y
'longitude'  =  in direz. X
'speed'      =
'direction'  =
'grade'      =  Qualità segnale
'panel'      =  Stato Motore: 0 (spento) 1 (acceso)
'event'      =  Tipo Evento
'vehtype'    =  Tipo Veicolo
'progressive'=  Odo viaggio
'millisec'   =  Millisec. RTC
'timedate_gps'= Data GPS
'distance'   =  Odo Assoluta
'id'         =  ????
_____________________________Vaiabili di GIANCARLO da Aggiungere

'Day_of_year' = Giorno dell'anno: 1 - 365
'Day_of_week' = Giorno della settimana: 0 - 6 (Domenica = 6)
'Delta_sec'   = Delta secondi calcolati dalla precedente registrazione
'Km'          = Km percorsi dalla preced. registrazione
'Km_prog'     = Km progressivi da inizio registrazioni
'Fl_Viag'
'Id_Viaggi'   = Nelle  cifre dopo "_" c'è il N* viaggio (0:N°Viaggi-1)
                nelle cifre precedenti c'è l'  Id_Veicolo
'TipoSosta'   =
'Univ'        =
'''

# elenco_Term_x_Freq = dati.idterm.value_counts(ascending=True) # Terminali ordinati per freq.
# elenco_Term_x_Id   = elenco_Term_x_Freq[ np.sort( elenco_Term_x_Freq.index ) ]

# Veic_considerati = elenco_Term_x_Id[ elenco_Term_x_Id > Registr_Min ].index

# tempo("==== I veicoli caricati sono: " + str(len(elenco_Term_x_Id))+"         ", t0)
# tempo("==== "+ str(len(Veic_considerati))+ " sono i veicoli utili             ", t0)

#==============================================================================
#  I N I Z I O   F O R     per caricare 1 veicolo x volta
#==============================================================================
N_elim = 0      # N° veicoli eliminati
Id_veic_elim    = []
Reg_veic_elim   = []
Causa_veic_elim = []

#____________________ Se si vuole analizzare 1 solo veicolo, ad es.il; 2725098
#____________________ Settare:


# get all ID terminal of Viasat data
# all_VIASAT_IDterminals = pd.read_sql_query(
#    ''' SELECT *
#        FROM public.obu''', conn_HAIG)

#### select only CARS
all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT *
        FROM public.idterm_portata
        /*WHERE vehtype = 1*/
         ''', conn_HAIG)

all_VIASAT_IDterminals['idterm'] = all_VIASAT_IDterminals['idterm'].astype('Int64')
all_VIASAT_IDterminals['vehtype'] = all_VIASAT_IDterminals['vehtype'].astype('Int64')
# all_VIASAT_IDterminals['anno'] = all_VIASAT_IDterminals['anno'].astype('Int64')
# all_VIASAT_IDterminals['portata'] = all_VIASAT_IDterminals['portata'].astype('Int64')
# make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
Veic_considerati = list(all_VIASAT_IDterminals.idterm.unique())
Veic_considerati = Veic_considerati[0:26541]
# Veic_considerati = Veic_considerati[20001:40000]
# Veic_considerati = Veic_considerati[0:300]

# track_ID = '4165307'
# for kv in  range( len(Veic_considerati[0:30]) ):
for kv, track_ID in enumerate(Veic_considerati):
    #==============================================================================
    #  C A R I C A   1   V E I C O L O  in   dati1v
    #______________________________________________________________________________

    #___________ Ordina per data e RICREA la colonna degli indici (index)

    # dati1v = dati[dati.idterm==id_veic ].sort_values('timedate')
    # print(track_ID)
    print ("veicolo num: ", kv)
    track_ID = str(track_ID)
    dati1v = pd.read_sql_query('''
                    SELECT * FROM public.dataraw
                    WHERE idterm = '%s' ''' % track_ID, conn_HAIG)
    dati1v = dati1v.sort_values('timedate')


    dati1v = pd.DataFrame(np.array(dati1v ),\
                          index=np.arange(len(dati1v)), columns= dati1v.columns )

    dati1v['timedate']   = dati1v.timedate   .astype('datetime64[ns]' )
    dati1v['speed']      = dati1v.speed      .astype('int64' )
    dati1v['progressive']= dati1v.progressive.astype('int64' )
    # dati1v['distance']   = dati1v.distance   .astype('int64' )

    dati1v['latitude']   = dati1v.latitude   .astype ( 'float64' )
    dati1v['longitude']  = dati1v.longitude  .astype( 'float64' )

    #___________________________________Crea variabili:____________________________
    #
    #    'Day_of_year' = Giorno dell'anno: 1 - 365
    #    'Delta_sec'   = Delta secondi calcolati dalla precedente registrazione
    #    'Km'          = Km percorsi dalla preced. registrazione
    #    'Km_prog'     = Km progressivi da inizio registrazioni
    #______________________________________________________________________________
    Day_of_year = np.zeros(len(dati1v), dtype='int')
    Day_of_week = np.zeros(len(dati1v), dtype='int')

    Delta_sec   = np.zeros(len(dati1v), dtype='int')

    for k in range(len(dati1v)):
        Day_of_year[k] = dati1v.timedate[k].dayofyear
        Day_of_week[k] = dati1v.timedate[k].dayofweek

    Delta_sec[1:] = (dati1v.timedate[1:].values - dati1v.timedate[:-1].values)/1e9

    dati1v['Day_of_year'] = Day_of_year
    dati1v['Day_of_week'] = Day_of_week
    dati1v['Delta_sec']   = Delta_sec

   # dati1v['Km'] = (dati1v.speed * dati1v.Delta_sec / 3600).round(4)

    ### modifica by Federico Karagulian 26 Febbraio 2021 #################
    ## --------------------------------------------------- ###############
    dati1v['last_progressive'] = dati1v.progressive.shift()  # <-------
    dati1v.last_progressive = dati1v.last_progressive.fillna(-1)  # <-------
    dati1v['km'] = dati1v.progressive - dati1v.last_progressive
    for i in range(len(dati1v)):
        if dati1v.km.iloc[i] > 0:
            dati1v.km.iloc[i] =  dati1v.km.iloc[i] /1000
        elif dati1v.km.iloc[i] < 0:
            dati1v.km.iloc[i] = 0


    dati1v['Km_prog'] = np.cumsum(dati1v.km)

    if Fl_Tempi:   tempo("==== Dopo creazione nuove Variabili        ", t0)

    #==============================================================================
    #   Calcolo e settaggio FLAG VIAGGIO: Fl_Viag
    #
    #   Se ci sono problemi: setta "Veic_da_Eliminare" con un codice diverso da ZERO
    #   ( il valore dei codici è nella routine: Setta_Fl_Viaggio()  )
    #==============================================================================

    if Fl_Tempi:   tempo("==== Prima del calcolo: Fl_Viag            ", t0)

    # from Routine_Viaggi import Setta_Fl_Viaggio
    Fl_Viag, Veic_da_Eliminare = Setta_Fl_Viaggio(dati1v)
    dati1v['Fl_Viag'] = Fl_Viag

    if Fl_Tempi:   tempo("==== Dopo del calcolo: Fl_Viag             ", t0)


    #==============================================================================
    #   Composizione e settaggio :  Id_Viaggi
    #
    #  Id_Viaggi è composto da: Id_term e N°Viaggio (saparati da '_')
    #            è tipo text
    #==============================================================================

    #Id_Viaggi = Crea_Id_Viaggi()
    dati1v['Id_Viaggi'] = Crea_Id_Viaggi(dati1v)


    #==================================================================
    #   Stampa 2 diagnostiche dei  VIAGGI (una sintetica e una complta)
    #==================================================================
    if Fl_Tab:
        print(' '*40, 'Tab. Preliminare SINTETICA')
        Tab_Sintetica(dati1v)

    #==================================================================
    #          FA PARTIRE DA ZERO:  DISTANCE e PROGRESSIVE
    #==================================================================
    # dati1v['distance']    = dati1v.distance - dati1v.distance [0]
    # dati1v['progressive'] = dati1v.progressive - dati1v.progressive [0]

    #...............................................
    if Veic_da_Eliminare == 0:       # = 0 il veicolo viene ELABORATO
                                # = 1  "     "   è eliminato

        #######################################################################
        #           I N I Z I O  di   Trova casa
        #######################################################################
        #                          Le soste sono classificate in: Notturne (20)
        #                                                    e Giornaliere (10)
        TipoSosta, pun_Notti_Strane_D, pun_Notti_Strane_O = \
                    Setta_Soste_Generiche_Notte_e_Giorno( Fl_Tab_Notte_Giorno )

        dati1v['TipoSosta'] = TipoSosta

        #__________________________________________Soste notturne fuori confine
        #_____________________________________ e nell'intervallo Marzo-Novembre
        Notti_Out = sum(dati1v.Fl_Viag[dati1v.TipoSosta==220] == 21) + \
                    sum(dati1v.Fl_Viag[dati1v.TipoSosta==220] == 22) + \
                    sum(dati1v.Fl_Viag[dati1v.TipoSosta==220] == 23)
        #________________________________________________Soste notturne interne
        Notti_In  = sum(dati1v.Fl_Viag[dati1v.TipoSosta==220] == 20)
        Notti_Tot = sum(dati1v.TipoSosta==220 )
        if Fl_Diag:
            print('Notti Interne =', Notti_In, '   Notti Esterne e Salto_Mesi =', Notti_Out)
            print('Notti Totali =', Notti_Tot)

        #==============================================================================
        #  Stampa le PRESENZE REALI (Dalle presenze sui giorni-anno)
        #___________________________________________________________________
        elenco_day_1v = dati1v.Day_of_year.value_counts(ascending=True) # Elenco GiorniAnno di.presenza veicolo
        elenco_day_1v = elenco_day_1v.sort_index()

        N_Presenze_Giornaliere = len(elenco_day_1v)
        if sum(dati1v.Day_of_year==0) > 0:
            N_Presenze_Giornaliere = N_Presenze_Giornaliere - 1
        N_Presenze_Giornaliere_Reali = sum(elenco_day_1v>3)
        if Fl_Diag:
            print('N_Presenze_Giornaliere:       ', N_Presenze_Giornaliere )
            print('N_Presenze_Giornaliere_Reali: ', N_Presenze_Giornaliere_Reali )

        #==============================================================================
        #        C A S A       Trova Coord.   e    Setta "TipoSosta"
        #______________________________________________________________________________
        #                                   T R O V A   coordinate PRIMA e SECONDA CASA
        Coord_1_Casa, Coord_2_Casa, N_Case = Calcola_Coord_Case()
        #                                              Inserisce Soste Anomale
        #                                              Dovute ad ACCENSIONI-SPEGNIMENTI
        dati1v.loc[ dati1v.Fl_Viag==5, 'TipoSosta'] = 5

        #                       S E T T A   soste PRIMA e SECONDA CASA in  "TipoSosta"
        #                                   In pratica quando sosta a casa CAMBIA la Sosta
        #                                   definita precedentemente Generica (220 o 210)
        #                                   in sosta 1 casa (221, 121, 211, 111) e
        #                                            2 casa (222, 122, 212, 112)
        if Coord_1_Casa[0] > 0 :

            #                         Cambia da sosta NOTTE  GENERICA in NOTTE  1° CASA
            Setta_Soste_Specifica( 220, 221, 121, Coord_1_Casa)
            #                         Cambia da sosta GIORNO GENERICA in GIORNO 1° CASA
            Setta_Soste_Specifica( 210, 211, 111, Coord_1_Casa)

            if Coord_2_Casa[0] > 0 :
                #                     Cambia da sosta NOTTE  GENERICA in NOTTE  2° CASA
                Setta_Soste_Specifica( 220, 222, 122, Coord_2_Casa)
                #                     Cambia da sosta GIORNO GENERICA in GIORNO 2° CASA
                Setta_Soste_Specifica( 210, 212, 112, Coord_2_Casa)

        #..............................................................................
        # Se le rimanenti soste generiche ( di giorno o notte ) sono al confine:
        #          ==> mettere codice CONFINE xx7
        #..............................................................................
        dati1v.loc[ (dati1v.Fl_Viag==21) & (dati1v.TipoSosta==220), 'TipoSosta'] = 227
        dati1v.loc[ (dati1v.Fl_Viag==11) & (dati1v.TipoSosta==120), 'TipoSosta'] = 127

        dati1v.loc[ (dati1v.Fl_Viag==21) & (dati1v.TipoSosta==210), 'TipoSosta'] = 217
        dati1v.loc[ (dati1v.Fl_Viag==11) & (dati1v.TipoSosta==110), 'TipoSosta'] = 117

        #==============================================================================
        #   L A V O R O     Trova Coord.   e    Setta "TipoSosta"
        #______________________________________________________________________________

        #                                 T R O V A   coordinate PRIMO e SECONDO LAVORO
        Coord_1_Lavoro, Coord_2_Lavoro, N_Lavori = Calcola_Coord_Lavori(Sosta_Lav_Min)

        #______________________S E T T A   soste PRIMO e SECONDO LAVORO in  "TipoSosta"
        if Coord_1_Lavoro[0] > 0 :
            #                         Cambia da sosta GIORNO GENERICA in GIORNO 1° LAVORO
            Setta_Soste_Specifica( 210, 215, 115, Coord_1_Lavoro)

            if Coord_2_Lavoro[0] > 0 :
                #                     Cambia da sosta GIORNO GENERICA in GIORNO 2° CASA
                Setta_Soste_Specifica( 210, 216, 116, Coord_2_Lavoro)

        #==============================================================================
        #   U N I V E R S I T A'     Trova Soste Università    e  Setta "Univ"
        #______________________________________________________________________________

        Univ = Trova_Luogo(Coord_Univ)
        dati1v['Univ'] = Univ

        #==============================================================================
        #       C H E C K   V E I C O L O
        #==============================================================================
        if Fl_Diag:
          print('\nCheck su Sosta minima riscontrata:',\
               min(dati1v.Delta_sec[ (dati1v.TipoSosta<120) &\
                                     (dati1v.TipoSosta>5) ]), 'sec')

        if Fl_Diag:
            print('Notti  1° Casa : ', sum(dati1v.TipoSosta==221))
            print('Notti  2° Casa : ', sum(dati1v.TipoSosta==222))
            print('Notti  Generica: ', sum(dati1v.TipoSosta==220))

            print('\nGiorni 1° Casa : ', sum(dati1v.TipoSosta==211))
            print('Giorni 2° Casa : ',   sum(dati1v.TipoSosta==212))
            print('Giorni 1° Lavoro : ', sum(dati1v.TipoSosta==215))
            print('Giorni 2° Lavoro: ',  sum(dati1v.TipoSosta==216))
            print('Giorni Generici: ',   sum(dati1v.TipoSosta==210))


        # freq = int(elenco_Term_x_Id [elenco_Term_x_Id.index==id_veic].values)
        # tempo(str(kv)+" == FINE Veic.N° "+str(id_veic)+" ( "+str(freq)+" reg.)    ", t0)

        #==============================================================================
        #     P R E P A R A   F I L E    V I A G G I   e lo aggiunge a   df_Viag
        #==============================================================================

        if sum( dati1v.Fl_Viag>19  ) > 2:          #  ALMENO 2 VIAGGI FINITI

            # crea i viaggidi 1 veicolo  da:  dati1v
            df_Viag_1v = Crea_df_Viaggi_1_Veicolo()

            df_Viag = df_Viag.append( df_Viag_1v )

            if Fl_Tab1 :     # STAMPA TAB VIAGGI CORRETTI
                print(' '*40, 'Tab. Finale più COMPLETA')
                Tab_Completa(dati1v)


            #==============================================================================
            #   aggiunge ai vettori  I N F O    V E I C O L O
            #==============================================================================

            # Aggiunge 1 nuovo veicolo ai vettori di INFO_Veic
            Aggiungi_1Veic_ai_Vett_di_INFO_Veic()

        else:
            Veic_da_Eliminare = 2        # 2= Il N° viaggi è < 3

            #N_elim = Aggiungi_1Veic_Eliminato(Veic_da_Eliminare, N_elim)
            N_elim = Aggiungi_1Veic_Eliminato('( Il N° viaggi è < 3 )', N_elim)

    else:
        #N_elim = Aggiungi_1Veic_Eliminato(Veic_da_Eliminare, N_elim)
        N_elim = Aggiungi_1Veic_Eliminato('(Punt. Arrive e Partenze hanno diverse dimensioni)', N_elim)


########################################
#       F I N E      F O R   id_veic
########################################

INFO_Veic   =    pd.DataFrame({'Id_Term': _Id_Term,
                        'Veh_Type'      : _Veh_Type,
                        'N_Giorni_Pres' : _N_Giorni_Pres,
                        'Km_Percorsi'   : _Km_Percorsi,
                        'N_Case'        : _N_Case,
                        'N_Lavori'      : _N_Lavori,
                        'Coord_1_Casa'  : _Coord_1_Casa,
                        'Coord_2_Casa'  : _Coord_2_Casa,
                        'Coord_1_Lavoro': _Coord_1_Lavoro,
                        'Coord_2_Lavoro': _Coord_2_Lavoro,
                        'Notti_In'      : _Notti_In,
                        'Notti_Out'     : _Notti_Out,

                        'Notti_1Casa'   : _Notti_1Casa,
                        'Notti_2Casa'   : _Notti_2Casa,
                        'Trans_Confine' : _Trans_Confine,
                        'N_Soste_Univ'  : _N_Soste_Univ,
                        'Giorni_al_1Lav': _Giorni_al_1Lav,
                        'Giorni_al_2Lav': _Giorni_al_2Lav })

df_Eliminati = pd.DataFrame({'Id_veic_elim'   : Id_veic_elim,
                             'Reg_veic_elim'  : Reg_veic_elim,
                             'Causa_veic_elim': Causa_veic_elim })

tempo("==== Sta salvando il file 'df_Viag'        ", t0)
# df_Viag.to_csv('df_Viag_0_10000_auto.csv', sep=',')
df_Viag.to_csv('df_Viag_0_26541.csv', sep=',')
# df_Viag.to_csv('df_Viag_20001_40000_auto.csv', sep=',')


tempo("==== Sta salvando il file 'INFO_Veic'      ", t0)
# INFO_Veic.to_csv('INFO_Veic_0_10000_auto.csv', sep=',')
INFO_Veic.to_csv('INFO_Veic_0_26541.csv', sep=',')
# INFO_Veic.to_csv('INFO_Veic_20001_40000_auto.csv', sep=',')

# df_Eliminati.to_csv('df_Eliminati_0_10000_auto.csv', sep=',')
df_Eliminati.to_csv('df_Eliminati_0_26541.csv', sep=',')
# df_Eliminati.to_csv('df_Eliminati_20001_40000_auto.csv', sep=',')



'''  
#  PLOT di punti caratteristici di un veicolo

ax51 = Plot_mappa(51, '1° Casa Giorno',   dati1v.TipoSosta==211, '.', 'm')
ax52 = Plot_mappa(52, '1° Casa Notte',    dati1v.TipoSosta==221, '.', 'k')
ax53 = Plot_mappa(53, '2° Casa Giorno',   dati1v.TipoSosta==212, '.', 'm')
ax54 = Plot_mappa(54, '2° Casa Notte',    dati1v.TipoSosta==222, '.', 'k')
ax55 = Plot_mappa(55, 'GENERICA Notte',   dati1v.TipoSosta==220, '.', 'k')
ax56 = Plot_mappa(56, 'GENERICA Giorno',  dati1v.TipoSosta==210, '.', 'k')
ax57 = Plot_mappa(57, 'TUTTE le soste',  dati1v.TipoSosta > 0, '.', 'r')


ax61 = Plot_mappa(61, '1° Lavoro',   dati1v.TipoSosta==215, '.', 'm')
ax62 = Plot_mappa(62, '2° Lavoro',   dati1v.TipoSosta==216, '.', 'm')

Plot_viaggio(ax51, 5157, 5182, '.', 'm')  # Alla Fig. "ax51" aggiunge il
#                                          tratto di viaggio da 5157 a 5182

'''

tempo("==== FINE                                  ", t0)
              