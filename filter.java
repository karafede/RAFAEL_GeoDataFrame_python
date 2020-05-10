// pannello=0 -> accensione
// pannello=1 -> in movimento
// pannello=2 ->spegnimento

                
				anomaly = "0123456".toCharArray();
                
                if (last_longi != -1L) {
                    lDist = Utility.computeMetricDistance(last_longi, longi, last_lati, lati);
                    timeDiff = dataTime.getTimeInMillis() - tr.getDataTime().get(tr.size() - 1).getTimeInMillis();
                }
                switch (pannello) {
                    case 0:
                        if (last_pannello == 0 || last_pannello == 1) {  // ERRORE: 0-0 e 1-0
                            if (tr != null) {
                                tr.getAnomaly().get(tr.size() - 1)[0] = 'E';   // set the anomaly to a certain parameter at the position "0" (create a new field called anomaly??)
                            }
                        }
                        tr = new Trip(); // aggiungo a trs solo dopo ogni new
                        trs.add(tr);
                        anomaly[0] = 'S';

                        break;
                    case 2:
                        anomaly[0] = 'E';
                        if (last_pannello == -1) {
                            tr = new Trip(); // aggiungo a trs solo dopo ogni new
                            trs.add(tr);
                        } else if (last_pannello == 2) { // ERRORE (2-2)
                            if (tr != null) {
                                tr.getAnomaly().get(tr.size() - 1)[0] = 'E';
                            }
                            tr = new Trip(); // aggiungo a trs solo dopo ogni new
                            trs.add(tr);
                        }
                        break;
                    case 1:
                        anomaly[0] = 'I';
                        if (last_pannello == -1) {
                            tr = new Trip(); // aggiungo a trs solo dopo ogni new
                            trs.add(tr);
                            anomaly[0] = 'S';
                        } else if (last_pannello == 2) { // ERRORE (2-1)
                            if (tr != null) {
                                tr.getAnomaly().get(tr.size() - 1)[0] = 'E';
                            }

                            tr = new Trip();
                            trs.add(tr);
                            anomaly[0] = 'S';
                        }
                        break;
                    default:
                        throw new Exception("Pannello non riconosciuto");
                    //break;
                    }
                // tempo (1-1) maggiore di 10 minuti
                if (tr != null && last_pannello <= 1 && pannello == 1 && timeDiff > 10 * 60000) {
                    anomaly[0] = 'S';
                    anomaly[4] = 'T';
                    tr.getAnomaly().get(tr.size() - 1)[0] = 'E';
                    tr.getAnomaly().get(tr.size() - 1)[4] = 'T';

                    tr = new Trip(); // aggiungo a trs solo dopo ogni new
                    trs.add(tr);
                }
                if (qualita <= 1) {
                    anomaly[1] = 'Q';
                } else if (qualita <= 2) {
                    anomaly[1] = 'q';
                }
                if (lDist > 0 && anomaly[0] != 'S') {
                    if (distanza / lDist < 0.9) {
                        anomaly[2] = 'c';
                    } else if (distanza / lDist > 10 && distanza > 2200) {
                        anomaly[3] = 'C';
                    }
                }
                if (timeDiff > 0 && 3.6 * 1000 * distanza / timeDiff > 250) {
                    anomaly[5] = 'V';
                }
                if (pannello != 0 && distanza > 10000) {
                    anomaly[0] = 'S';
                    anomaly[6] = 'D';
                    tr.getAnomaly().get(tr.size() - 1)[0] = 'E';
                    tr.getAnomaly().get(tr.size() - 1)[6] = 'D';

                    tr = new Trip(); // aggiungo a trs solo dopo ogni new
                    trs.add(tr);

                } else if (pannello != 0 && distanza <= 0) {
                    anomaly[6] = 'd';
                }
				
//public boolean getAnomaly()
//                   throws java.rmi.RemoteException
//Get the value of anomaly
//Returns:
//anomaly.

////// switch-case (“dato un valore per c, procedi ad eseguire le istruzioni a partire dal case contrassegnato dallo stesso valore”)
//switch(c) {
//case value1:
//...
//break;
//case value2:
//...
//break;
// eventuali altri case
//case valueN:
//...
//default:
//}