/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GLDC_Queue;

import Planificador.JavaJob;
import static java.lang.Class.forName;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author dg3861
 */
public class Queu_BajaNatural {
    
      public static void main(String[] args) {

          
             try {
              System.out.println("Actualización Baja Natural PW_QUEUE_RMSINTGEO_BAJAS");
             
             String querytruncate= "truncate table PW_QUEUE_RMSINTGEO_BAJAS";   
              
             String queryactualizaintergeo = "INSERT INTO PW_QUEUE_RMSINTGEO_BAJAS "
                     + "WITH qaux as(\n" +
                        "select distinct a.request_id as caso,d.ticket_number,\n" +
                        "nvl(substr(c.liid,5,12),h.value) as DN, h.value msisdn, replace(q.name,',',' ') as Agencia,replace(concat(concat(r.firstname,' '),r.lastname),',',' ') as Solicitante,\n" +
                        "(from_tz(CAST (b.startdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') as inicio,\n" +
                        "(from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') as fin,f.name as Tipo,\n" +
                        "replace(replace(replace(g.code,'EVENT_CASE_EARLY_DEACTIVATION_REQUEST_RECEIVED','BAJA ANTICIPADA'),'EVENT_CASE_EXTENSION_REQUEST_RECEIVED','PRORROGA'),'EVENT_DPR_CREATED_FROM_OFFREQ','ALTA') AS SUBTIPO,\n" +
                        "(from_tz(CAST (g.createddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')as createddatesubtipo,\n" +
                        "round((round(trunc(1000 * (extract(second from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City'))\n" +
                        "    + 60 * (extract(minute from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City'))\n" +
                        "      + 60 * (extract(hour from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City'))\n" +
                        "        + 24 * (extract(day from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')))))))/3600000,2))/24,2) as VENCE_DIAS\n" +
                        "from event_history@RMS a, target_details@RMS b, target_query@RMS c,request@RMS d, offline_request@RMS e, req_type_def@RMS f,agency@RMS q, requestor@RMS r, event_history@RMS g, subscriber_identifr_dtl@RMS h\n" +
                        "--where (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')>= (from_tz(CAST (sysdate AS TIMESTAMP),'CST') at  TIME ZONE 'America/Mexico_City')\n" +
                        "where a.request_id=b.request_id\n" +
                        "and a.request_id not in (select distinct request_id from event_history@RMS where request_id=a.request_id and code in ('EVENT_CASE_EXTENSION_REQUEST_RECEIVED'))\n" +
                        "and c.target_id=b.targetid\n" +
                        "and e.ticket_number=d.ticket_number\n" +
                        "and d.request_id=a.request_id\n" +
                        "and f.req_type_def_id=d.req_type_def_id\n" +
                        "and a.request_id not in\n" +
                        "(select td1.request_id from target_details@RMS td1,REQ_TYPE_REASONS_CONFIG@RMS rc1 where td1.request_id=b.request_id and td1.category_status_reason_id=rc1.id)\n" +
                        "and ( f.name like 'Intervenci%n Legal%' OR f.name LIKE '%Localizaci%n Geogr%fica%')\n" +
                        "and q.id=r.agency_id\n" +
                        "and r.id=e.requestor_id\n" +
                        "and g.request_id=b.request_id\n" +
                        "and g.code in ('EVENT_RESPONSE_DOCUMENT_UPLOAD_TO_PACKAGE')\n" +
                        "--and a.createddate=(select min(createddate) from event_history@RMS where request_id=a.request_id and code='EVENT_DPR_CREATED_FROM_OFFREQ')\n" +
                        "AND b.targetid=h.SUBSCRIBER_IDENTIFIER_ID\n" +
                        "UNION\n" +
                        "select distinct a.request_id as caso,d.ticket_number,\n" +
                        "nvl(substr(c.liid,5,12),h.value)  as DN, h.value msisdn, replace(q.name,',',' ') as Agencia,concat(concat(r.firstname,' '),r.lastname) as Solicitante,\n" +
                        "(from_tz(CAST (b.startdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') as inicio,\n" +
                        "(from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') as fin,f.name as Tipo,\n" +
                        "replace(replace(replace(g.code,'EVENT_CASE_EARLY_DEACTIVATION_REQUEST_RECEIVED','BAJA ANTICIPADA'),'EVENT_CASE_EXTENSION_REQUEST_RECEIVED','PRORROGA'),'EVENT_DPR_CREATED_FROM_OFFREQ','ALTA') AS SUBTIPO,\n" +
                        "(from_tz(CAST (g.createddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')as createddatesubtipo,\n" +
                        "round((round(trunc(1000 * (extract(second from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City'))\n" +
                        "    + 60 * (extract(minute from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City'))\n" +
                        "      + 60 * (extract(hour from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City'))\n" +
                        "        + 24 * (extract(day from (from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') - (from_tz(CAST (sysdate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')))))))/3600000,2))/24,2) as VENCE_DIAS\n" +
                        "from event_history@RMS a, target_details@RMS b, target_query@RMS c,request@RMS d, offline_request@RMS e, req_type_def@RMS f,agency@RMS q, requestor@RMS r, event_history@RMS g, subscriber_identifr_dtl@RMS h\n" +
                        "--where \n" +
                        "--(from_tz(CAST (b.enddate AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')>= (from_tz(CAST (sysdate AS TIMESTAMP),'CST') at  TIME ZONE 'America/Mexico_City')\n" +
                        "where a.request_id=b.request_id\n" +
                        "and c.target_id=b.targetid\n" +
                        "and e.ticket_number=d.ticket_number\n" +
                        "and d.request_id=a.request_id\n" +
                        "and f.req_type_def_id=d.req_type_def_id\n" +
                        "and a.request_id not in\n" +
                        "(select td1.request_id from target_details@RMS td1,REQ_TYPE_REASONS_CONFIG@RMS rc1 where td1.request_id=b.request_id and td1.category_status_reason_id=rc1.id)\n" +
                        "and ( f.name like 'Intervenci%n Legal%' OR f.name LIKE '%Localizaci%n Geogr%fica%')\n" +
                        "and q.id=r.agency_id\n" +
                        "and r.id=e.requestor_id\n" +
                        "and g.request_id=b.request_id\n" +
                        "and g.code in ('EVENT_RESPONSE_DOCUMENT_UPLOAD_TO_PACKAGE')\n" +
                        "--and a.createddate=(select min(createddate) from event_history@RMS where request_id=a.request_id and code='EVENT_CASE_EXTENSION_REQUEST_RECEIVED')\n" +
                        "AND b.targetid=h.SUBSCRIBER_IDENTIFIER_ID)\n" +
                        "SELECT * FROM qaux\n" +
                        "--WHERE caso ='83770'\n" +
                        "--AND ticket_number='MX22021149WUOA'\n" +
                        "WHERE  fin < CREATEDDATESUBTIPO\n" +
                        "AND TRUNC(CREATEDDATESUBTIPO) = TRUNC( SYSDATE-22) --- PARA QUE COINCIDA CUANDO LO SUBAN EL DOCUMENTO EN RMS...";
              
               
             String insertabitacora= "   INSERT INTO   DG3861.PW_QUEUE_BITACORA   \n" +
                                "   SELECT NULL, B.ID_TOKEN, A.NTICKET, B.ID_USUARIO, B.ID_LOGIN, 6, B.FECHA_ASIGNACION, sysdate, sysdate, B.ID_PRIORIDAD, B.BAND_REASIGNADO, sysdate  FROM PW_QUEUE_TOKEN  A\n" +
                                "						INNER JOIN (\n" +
                                "								SELECT * FROM DG3861.PW_QUEUE_BITACORA   \n" +
                                "								WHERE (ID_TOKEN, ESTATUS) IN (\n" +
                                "								SELECT ID_TOKEN, MAX(ESTATUS) ULTEST FROM DG3861.PW_QUEUE_BITACORA       \n" +
                                "								GROUP BY ID_TOKEN)\n" +
                                "								) B\n" +
                                "								ON A.ID_TOKEN = B.ID_TOKEN \n" +
                                "									INNER JOIN ( SELECT DISTINCT CASO , TICKET_NUMBER FROM DG3861.PW_QUEUE_RMSINTGEO_BAJAS)   C\n" +
                                "									 ON A.NTICKET = C.TICKET_NUMBER\n" +
                                "									   AND A.ID_CASO = C.CASO								\n" +
                                "				WHERE TIPO_SOLICITUD IN ('Intervención Legal Baja Natural','Localización Geográfica Baja Natural')\n" +
                                "				AND b.ESTATUS=3 \n" +
                                "				";

        
              Herramientas.Conexion con = new Herramientas.Conexion("DG3861_DGLDCSTG");
              ResultSet rSet;
              
        
               
               rSet = con.ejecutarQuery(querytruncate);
               rSet = con.ejecutarQuery(queryactualizaintergeo);
               rSet = con.ejecutarQuery(insertabitacora);
        
             
                
                      con.con.close();  
                     
                      
                      // 1 ---6 --- 5  
                      // 2 ---4 --- 5   
                      
                      //4 usuario ct 0 - 3
                        
            
                 con.con.close();
              
        
        
          } catch (SQLException ex) {
              //System.out.println("Piscachas con los querys RMS QUEUE"+ex);
//              Logger.getLogger(Queu_Temis.class.getName()).log(Level.SEVERE, null, "Piscachas con los querys RMS QUEUE"+ex);
             
          }
        
          
          
      }
    
    
}
