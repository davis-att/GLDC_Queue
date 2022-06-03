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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author dg3861
 */
public class Queu_ActualizaRMS_job implements Job {
    
   @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

          
       
          try {
              System.out.println("Actualización RMS");
              
              
           String querytruncate = "TRUNCATE TABLE DG3861.PW_QUEUE_RMS_SOLICITUDES";
              String queryinsert ="INSERT INTO DG3861.PW_QUEUE_RMS_SOLICITUDES \n" +
                                        " WITH queryrmsaux AS (\n" +
                                        "SELECT 	distinct\n" +
                                        "	a.ticket_number,\n" +
                                        "	substr(a.subject,1,20) token,\n" +
                                        "  -- null as tipo,\n" +
                                        "   K.name as tipo,\n" +
                                        "  	a.subject asunto,\n" +
                                        "  	a.status, \n" +
                                        "  	(from_tz(CAST (nvl(a.STATUS_MODIFIED_DATE, a.created_date) AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')   as fecha_estatus,\n" +
                                        "  	CASE WHEN UPPER(a.status ) = 'NEW' THEN 4\n" +
                                        "				WHEN UPPER(a.status ) IN ('REGISTERED', 'PROVISIONING','SEARCHING')  THEN 5\n" +
                                        "				WHEN UPPER(a.status ) IN ('PREPARING_RESPONSE', 'COMPLETED','MULTIPLE') THEN 6\n" +
//                                        "				WHEN UPPER(a.status ) = 'NEW' THEN 1\n" +
                                        "				WHEN UPPER(a.status ) IN ('CANCELLED', 'DELETED','DUPLICATE','REJECTED') THEN 6\n" +
                                        "				WHEN UPPER(a.status) = 'REJECTED' THEN 21\n" +
                                        "				WHEN UPPER(a.status ) = 'CANCELLED' THEN 22\n" +
                                        "				WHEN UPPER(a.status ) = 'DUPLICATE' THEN 23\n" +
                                        "				WHEN UPPER(a.status ) = 'DELETED' THEN 24 ELSE 41 END ESTATUS_QUEUE,\n" +
                                        "  	--request_id,  	\n" +
                                        "  	nvl(e.code ,i.code) code,\n" +
                                        "  		(from_tz(CAST (  nvl(e.createddate,i.createddate)   AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City') created_date,  			\n" +
                                        "  	nvl(f.username , j.username) username, h.ticket_number ticket_number_asc, sysdate fecha_registro--, g.REQUEST_ID caso\n" +
                                        "  	--,i.code code_offl,\n" +
                                        "  	--(from_tz(CAST (i.createddate  AS TIMESTAMP),'GMT') at  TIME ZONE 'America/Mexico_City')  create_offl,\n" +
                                        "  	--j.username username_offl\n" +
                                        "  	FROM (SELECT ticket_number,status, substr(subject,1,500) subject, STATUS_MODIFIED_DATE, created_date, OFFLINE_REQUEST_ID,IR_TYPE from offline_request@rms) a\n" +
                                        "  			LEFT JOIN  (SELECT ticket_number, request_id, req_type_def_id FROM request@rms) b\n" +
                                        "  				 ON  a.ticket_number = b.ticket_number\n" +
                                        "  				 LEFT JOIN  (SELECT request_id, code,userid,CREATEDDATE  FROM event_history@rms) e\n" +
                                        "  				  ON  e.request_id=b.request_id\n" +
                                        "  				  AND e.code='EVENT_CASE_PROCEEDED'\n" +
                                        "  				  LEFT JOIN (SELECT userid, username  FROM lisuser@rms) f\n" +
                                        "  				  ON e.userid = f.userid		\n" +
                                        "  				   LEFT JOIN  req_offline_xref@rms g\n" +
                                        "  				 	 ON  g.OFFLINE_REQUEST_ID=a.OFFLINE_REQUEST_ID\n" +
                                        "  				 	  LEFT JOIN  (SELECT ticket_number, request_id FROM request@rms) h\n" +
                                        "  						 ON  g.REQUEST_ID = h.REQUEST_ID\n" +
                                        "  				 	 		 LEFT JOIN  (SELECT request_id, code,userid,CREATEDDATE  FROM event_history@rms) i\n" +
                                        "  				 			 	ON  h.request_id=i.request_id\n" +
                                        "  							  		AND i.code='EVENT_CASE_PROCEEDED'\n" +
                                        "  				 	 			LEFT JOIN (SELECT userid, username  FROM lisuser@rms) j\n" +
                                        "  				 						 ON i.userid = j.userid	\n" +
                                        "  				 						 LEFT JOIN (SELECT req_type_def_id, name FROM  req_type_def@rms) k\n" +
                                        "  				 						 	ON b.req_type_def_id = k.req_type_def_id\n" +
                                        "--WHERE  a.ticket_number='MX21120722WXIM'\n" +
                                        "where a.created_date > sysdate -30  				 						 \n" +
                                        "AND upper(a.IR_TYPE)= 'EMAIL')\n" +
                                        "SELECT * FROM queryrmsaux\n" +
                                        "WHERE (TICKET_NUMBER, NVL(CREATED_DATE, FECHA_ESTATUS )) IN (\n" +
                                        "(SELECT TICKET_NUMBER, MAX(NVL(CREATED_DATE, FECHA_ESTATUS )) FROM queryrmsaux\n" +
                                        "GROUP BY TICKET_NUMBER))";
              
              /*
              String queryinstoken="INSERT INTO DG3861.PW_QUEUE_TOKEN \n" +
" SELECT 'AT220506122952RE'||dummy_seq.nextval, '1234/DUMMY', 3, tipo, \n" +
" CASE WHEN tipo LIKE 'Intervención Legal' THEN 1\n" +
" WHEN tipo LIKE 'Localización Geográfica' THEN 1\n" +
" WHEN tipo LIKE 'Ordenes de Información Histórica' THEN 3\n" +
" ELSE 4 END, 'Email', nvl(created_date,FECHA_ESTATUS), TICKET_NUMBER ,0,null,null,null,null FROM ( select * from DG3861.PW_QUEUE_RMS_SOLICITUDES WHERE ESTATUS_QUEUE = 1)\n" +
" WHERE TICKET_NUMBER NOT IN ( SELECT DISTINCT TIPO_IDENTIFICADOR FROM DG3861.PW_QUEUE_TOKEN where TIPO_IDENTIFICADOR is not null)" ;

          String queryinsbitacora=" INSERT INTO DG3861.PW_QUEUE_BITACORA \n" +
" (ID_TOKEN, TICKET_NUMBER, ID_USUARIO, ID_LOGIN, ESTATUS, FECHA_ASIGNACION, FECHA_ESTATUS, FECHA_CIERRE, ID_PRIORIDAD, BAND_REASIGNADO, FECHA_REGISTRO)\n" +
" SELECT ID_TOKEN, NULL, NULL, NULL, 1, NULL, FECHA_REGISTRO, NULL, ID_PRIORIDAD, 0, sysdate FROM DG3861.PW_QUEUE_TOKEN \n" +
" WHERE TIPO_IDENTIFICADOR NOT IN ( SELECT DISTINCT TICKET_NUMBER FROM DG3861.PW_QUEUE_BITACORA WHERE TICKET_NUMBER IS NOT NULL )";

          String queryinsbitactual="   INSERT INTO DG3861.PW_QUEUE_BITACTUAL \n" +
" (ID_TOKEN, TICKET_NUMBER, ID_USUARIO, ID_LOGIN, ESTATUS, FECHA_ASIGNACION, FECHA_ESTATUS, FECHA_CIERRE, ID_PRIORIDAD, BAND_REASIGNADO, FECHA_REGISTRO)\n" +
" SELECT ID_TOKEN, NULL, NULL, NULL, 1, NULL, FECHA_REGISTRO, NULL, ID_PRIORIDAD, 0, sysdate FROM DG3861.PW_QUEUE_TOKEN \n" +
" WHERE TIPO_IDENTIFICADOR NOT IN ( SELECT DISTINCT TICKET_NUMBER FROM DG3861.PW_QUEUE_BITACORA WHERE TICKET_NUMBER IS NOT NULL )";
              */
            
/*              
              String querymrgbitacora=   "MERGE INTO DG3861.PW_QUEUE_BITACORA x\n" +
"USING (\n" +
"SELECT A.TICKET_NUMBER, A.TIPO, A.ASUNTO, A.STATUS,\n" +
"		CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN  A.CREATED_DATE\n" +
"			ELSE A.FECHA_ESTATUS END FECHA_ESTATUS, \n" +
"		CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN  6\n" +
"			ELSE A.ESTATUS_QUEUE END ESTATUS_QUEUE, \n" +
"    A.CODE, A.CREATED_DATE, A.USERNAME, A.TICKET_NUMBER_ASC, \n" +
"	B.ID_TOKEN, C.ID_USUARIO, C.ID_LOGIN, C.FECHA_ASIGNACION, C.ID_PRIORIDAD \n" +
"	FROM DG3861.PW_QUEUE_RMS_SOLICITUDES A \n" +
"	INNER JOIN  PW_QUEUE_TOKEN B\n" +
"		ON A.TICKET_NUMBER = B.TIPO_IDENTIFICADOR\n" +
"			INNER JOIN (SELECT ID_TOKEN,ID_USUARIO, ID_LOGIN, FECHA_ASIGNACION, ID_PRIORIDAD FROM PW_QUEUE_BITACORA\n" +
"			WHERE (ID_TOKEN,FECHA_REGISTRO ) IN (\n" +
" 	SELECT ID_TOKEN, max(FECHA_REGISTRO) FROM pw_queue_bitacora 	 \n" +
" 	GROUP BY id_token)) C \n" +
"			ON B.ID_TOKEN = C.ID_TOKEN\n" +
" 	--WHERE b.id_token='AT220506122952RE1485'			\n" +
"			) y\n" +
"ON (x.ID_TOKEN  = y.ID_TOKEN AND to_char(x.estatus)  = to_char(y.ESTATUS_QUEUE) )\n" +
"WHEN MATCHED THEN\n" +
"    UPDATE SET x.TICKET_NUMBER = y.TICKET_NUMBER\n" +
"WHEN NOT MATCHED THEN\n" +
"	  INSERT \n" +
"  		(ID_TOKEN, TICKET_NUMBER, ID_USUARIO, ID_LOGIN, ESTATUS, FECHA_ASIGNACION, FECHA_ESTATUS, FECHA_CIERRE, ID_PRIORIDAD, BAND_REASIGNADO, FECHA_REGISTRO)\n" +
"       VALUES( Y.ID_TOKEN, Y.TICKET_NUMBER, Y.ID_USUARIO, Y.ID_LOGIN, Y.ESTATUS_QUEUE, Y.FECHA_ASIGNACION, Y.FECHA_ESTATUS, CASE WHEN Y.ESTATUS_QUEUE = 6 THEN sysdate ELSE NULL end, Y.ID_PRIORIDAD, 0, sysdate)  ";

*/

            String querymrgbitacora=   "MERGE INTO DG3861.PW_QUEUE_BITACORA x\n" +
"USING (\n" +
"SELECT A.TICKET_NUMBER, A.TIPO, A.ASUNTO, A.STATUS,\n" +
"		CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN  A.CREATED_DATE\n" +
"			ELSE A.FECHA_ESTATUS END FECHA_ESTATUS, \n" +
"		CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN  6\n" +
"			ELSE A.ESTATUS_QUEUE END ESTATUS_QUEUE, \n" +
"    A.CODE, A.CREATED_DATE, A.USERNAME, A.TICKET_NUMBER_ASC, \n" +
"	B.ID_TOKEN, C.ID_USUARIO, C.ID_LOGIN, C.FECHA_ASIGNACION, C.ID_PRIORIDAD \n" +
"	FROM DG3861.PW_QUEUE_RMS_SOLICITUDES A \n" +
"	INNER JOIN  PW_QUEUE_TOKEN B\n" +
"		ON RTRIM (A.TOKEN) = B.ID_TOKEN\n" +
"			INNER JOIN (SELECT ID_TOKEN,ID_USUARIO, ID_LOGIN, FECHA_ASIGNACION, ID_PRIORIDAD FROM PW_QUEUE_BITACORA\n" +
"			WHERE (ID_TOKEN,FECHA_REGISTRO ) IN (\n" +
" 	SELECT ID_TOKEN, max(FECHA_REGISTRO) FROM pw_queue_bitacora 	 \n" +
" 	GROUP BY id_token)) C \n" +
"			ON B.ID_TOKEN = C.ID_TOKEN\n" +
" 	--WHERE b.id_token='AT220506122952RE1485'			\n" +
"			) y\n" +
"ON (x.ID_TOKEN  = y.ID_TOKEN AND to_char(x.estatus)  = to_char(y.ESTATUS_QUEUE) )\n" +
"WHEN MATCHED THEN\n" +
"    UPDATE SET x.TICKET_NUMBER = y.TICKET_NUMBER\n" +
"WHEN NOT MATCHED THEN\n" +
"	  INSERT \n" +
"  		(ID_TOKEN, TICKET_NUMBER, ID_USUARIO, ID_LOGIN, ESTATUS, FECHA_ASIGNACION, FECHA_ESTATUS, FECHA_CIERRE, ID_PRIORIDAD, BAND_REASIGNADO, FECHA_REGISTRO)\n" +
"       VALUES( Y.ID_TOKEN, Y.TICKET_NUMBER, Y.ID_USUARIO, Y.ID_LOGIN, Y.ESTATUS_QUEUE, Y.FECHA_ASIGNACION, Y.FECHA_ESTATUS, CASE WHEN Y.ESTATUS_QUEUE = 6 THEN sysdate ELSE NULL end, Y.ID_PRIORIDAD, 0, sysdate)  ";



        String querymrgbitactual="MERGE INTO DG3861.PW_QUEUE_BITACTUAL x\n" +
                    "USING (\n" +
                    "			SELECT * FROM DG3861.PW_QUEUE_BITACORA\n" +
                    "					WHERE (ID_TOKEN,FECHA_REGISTRO ) IN (\n" +
                    "		 	SELECT ID_TOKEN, max(FECHA_REGISTRO) FROM DG3861.pw_queue_bitacora 	 \n" +
                    "		 	GROUP BY id_token)			\n" +
                    "			) y\n" +
                    "ON (x.ID_TOKEN  = y.ID_TOKEN)\n" +
                    "WHEN MATCHED THEN\n" +
                    "    UPDATE SET \n" +
                    "    x.TICKET_NUMBER = y.TICKET_NUMBER, \n" +
                    "    x.ID_USUARIO = y.ID_USUARIO, \n" +
                    "    x.ID_LOGIN = y.ID_LOGIN, \n" +
                    "    x.ESTATUS = y.ESTATUS, \n" +
                    "    x.FECHA_ASIGNACION = y.FECHA_ASIGNACION, \n" +
                    "    x.FECHA_ESTATUS = y.FECHA_ESTATUS, \n" +
                    "    x.FECHA_CIERRE = y.FECHA_CIERRE, \n" +
                    "    x.ID_PRIORIDAD = y.ID_PRIORIDAD, \n" +
                    "    x.BAND_REASIGNADO = y.BAND_REASIGNADO,\n" +
                    "    x.ID_QUEUE = y.ID_QUEUE";

/*        
          String querymrgbitest3= "MERGE INTO DG3861.PW_QUEUE_BITACORA x\n" +
"USING (\n" +
"SELECT A.TICKET_NUMBER, A.TIPO, A.ASUNTO, A.STATUS,\n" +
" CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN A.CREATED_DATE\n" +
" ELSE A.FECHA_ESTATUS END FECHA_ESTATUS, \n" +
" CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN 6\n" +
" ELSE A.ESTATUS_QUEUE END ESTATUS_QUEUE, \n" +
" A.CODE, A.CREATED_DATE, A.USERNAME, A.TICKET_NUMBER_ASC, \n" +
" B.ID_TOKEN, C.ID_USUARIO, C.ID_LOGIN, C.FECHA_ASIGNACION, C.ID_PRIORIDAD \n" +
" FROM DG3861.PW_QUEUE_RMS_SOLICITUDES A \n" +
" INNER JOIN PW_QUEUE_TOKEN B\n" +
" ON A.TICKET_NUMBER = B.TIPO_IDENTIFICADOR\n" +
" INNER JOIN (SELECT ID_TOKEN,ID_USUARIO, ID_LOGIN, FECHA_ASIGNACION, ID_PRIORIDAD FROM PW_QUEUE_BITACORA\n" +
" WHERE (ID_TOKEN,FECHA_REGISTRO ) IN (\n" +
" SELECT ID_TOKEN, max(FECHA_REGISTRO) FROM pw_queue_bitacora \n" +
" GROUP BY id_token)) C \n" +
" ON B.ID_TOKEN = C.ID_TOKEN\n" +
" --WHERE b.id_token='AT220506122952RE1485' \n" +
" ) y\n" +
"ON (x.ID_TOKEN = y.ID_TOKEN AND x.ESTATUS in (2, 3))\n" +
"WHEN MATCHED THEN\n" +
" UPDATE SET x.TICKET_NUMBER = y.TICKET_NUMBER";
        */

//ACTUALIZA UNICMANETE EL TICKET NUMBER PARA ESTATUS 2Y3
 String querymrgbitest3="MERGE INTO DG3861.PW_QUEUE_BITACORA x\n" +
"USING (\n" +
"SELECT A.TICKET_NUMBER, A.TIPO, A.ASUNTO, A.STATUS,\n" +
" CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN A.CREATED_DATE\n" +
" ELSE A.FECHA_ESTATUS END FECHA_ESTATUS, \n" +
" CASE WHEN A.CODE='EVENT_CASE_PROCEEDED' THEN 6\n" +
" ELSE A.ESTATUS_QUEUE END ESTATUS_QUEUE, \n" +
" A.CODE, A.CREATED_DATE, A.USERNAME, A.TICKET_NUMBER_ASC, \n" +
" B.ID_TOKEN, C.ID_USUARIO, C.ID_LOGIN, C.FECHA_ASIGNACION, C.ID_PRIORIDAD \n" +
" FROM DG3861.PW_QUEUE_RMS_SOLICITUDES A \n" +
" INNER JOIN PW_QUEUE_TOKEN B\n" +
" ON RTRIM(A.TOKEN) = B.ID_TOKEN\n" +
" INNER JOIN (SELECT ID_TOKEN,ID_USUARIO, ID_LOGIN, FECHA_ASIGNACION, ID_PRIORIDAD FROM PW_QUEUE_BITACORA\n" +
" WHERE (ID_TOKEN,FECHA_REGISTRO ) IN (\n" +
" SELECT ID_TOKEN, max(FECHA_REGISTRO) FROM pw_queue_bitacora \n" +
" GROUP BY id_token)) C \n" +
" ON B.ID_TOKEN = C.ID_TOKEN\n" +
" --WHERE b.id_token='AT220506122952RE1485' \n" +
" ) y\n" +
"ON (x.ID_TOKEN = y.ID_TOKEN AND x.ESTATUS in (2, 3))\n" +
"WHEN MATCHED THEN\n" +
" UPDATE SET x.TICKET_NUMBER = y.TICKET_NUMBER";


              //Trae información de la tabla SCHEDULE de DATA_P
              Herramientas.Conexion con = new Herramientas.Conexion("DG3861_DGLDCSTG");
              ResultSet rs;
              
              rs = con.ejecutarQuery(querytruncate);
              rs = con.ejecutarQuery(queryinsert);
             // rs = con.ejecutarQuery(queryinstoken);
             // rs = con.ejecutarQuery(queryinsbitacora);
             // rs = con.ejecutarQuery(queryinsbitactual);              
              rs = con.ejecutarQuery(querymrgbitacora);
              rs = con.ejecutarQuery(querymrgbitactual);
              rs = con.ejecutarQuery(querymrgbitest3);
              
              con.con.close();
          } catch (SQLException ex) {
              //System.out.println("Piscachas con los querys RMS QUEUE"+ex);
            //  Logger.getLogger(Queu_ActualizaRMS.class.getName()).log(Level.SEVERE, null, "Piscachas con los querys RMS QUEUE"+ex);
             
          }
        
          
          
      }
    
    
}
