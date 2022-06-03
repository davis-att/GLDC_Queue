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
public class Queu_Temis_job implements Job {
    
   @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

          
             try {
              System.out.println("Actualización Temis");
               List<SetPlayTemis> disponibilidad = new ArrayList<>();
               List<SetPlayTemis> listatrabajo = new ArrayList<>();
               List<SetPlayTemis> cargatrabajo = new ArrayList<>();
               ArrayList insertsfinales = new ArrayList();
              
               
              /*String querylistatrabajo = "SELECT ID_QUEUE, ID_TOKEN, ID_PRIORIDAD,  FECHA_REGISTRO\n" +
                                    "	FROM DG3861.PW_QUEUE_BITACORA\n" +
                                    "	WHERE ID_TOKEN IN\n" +
                                    "			(\n" +
                                    "			SELECT ID_TOKEN\n" +
                                    "				FROM DG3861.PW_QUEUE_BITACORA\n" +
                                    "				WHERE ESTATUS = 1\n" +
                                    "			MINUS \n" +
                                    "			SELECT ID_TOKEN\n" +
                                    "			FROM DG3861.PW_QUEUE_BITACORA\n" +
                                    "			WHERE ( ESTATUS <>  1 OR ESTATUS IS NULL) \n" +
                                    "			)\n" +
                                    "ORDER BY ID_PRIORIDAD, FECHA_REGISTRO ";
              */
             String querylistatrabajo = "SELECT d.ID_QUEUE, a.ID_TOKEN , b.id_prioridadatencion, d.FECHA_REGISTRO  \n" +
                                                "FROM PW_QUEUE_TOKEN A\n" +
                                                "	INNER JOIN pw_queue_priori_atencion B\n" +
                                                "	   ON A.ID_PRIORIDAD  = B.ID_PRIORIDAD \n" +
                                                "			INNER JOIN   PW_QUEUE_TIPOSOL C\n" +
                                                "				ON a.tipo_solicitud = c.NOMBRE_ROL\n" +
                                                "				   AND c.id_sol=b.id_solicitud\n" +
                                                "				   	 INNER JOIN PW_QUEUE_BITACTUAl d\n" +
                                                "				   	   ON  a.id_token = d.id_token\n" +
                                                "WHERE estatus=1				   	   \n" +
                                                "ORDER BY b.id_prioridadatencion ,FECHA_REGISTRO";
              
               
               String querycargatrabajo = "SELECT C.ID_USUARIO , nvl(a.TOTAL_ATENDIENDO,0) TOTAL_ATENDIENDO, \n" +
                        "	nvl(b.MAX_FECHA_ASGINACION, sysdate-1) MAX_FECHA_ASGINACION, DQUEUE  FROM \n" +
                        "PW_QUEUE_CRONO c\n" +
                        "LEFT JOIN (\n" +
                        "		SELECT ID_USUARIO, COUNT(1) TOTAL_ATENDIENDO FROM (SELECT * FROM pw_queue_bitacora \n" +
                        "WHERE (ID_TOKEN,FECHA_REGISTRO ) IN (\n" +
                        " 	SELECT ID_TOKEN, max(FECHA_REGISTRO) FROM pw_queue_bitacora 	 \n" +
                        " 	GROUP BY id_token)\n" +
                        ")\n" +
                        "		WHERE ESTATUS IN (5, 3, 2,4)\n" +
                        "		AND ID_USUARIO IS NOT NULL\n" +
                        "		GROUP BY ID_USUARIO\n" +
                        "		) a\n" +
                        "		ON a.id_usuario = c.id_usuario				\n" +
                        "	LEFT JOIN (\n" +
                        "		SELECT ID_USUARIO, MAX(FECHA_ASIGNACION) MAX_FECHA_ASGINACION FROM DG3861.PW_QUEUE_BITACORA\n" +
                        "		--WHERE ESTATUS = 6\n" +
                        "		WHERE ID_USUARIO IS NOT NULL\n" +
                        "		GROUP BY ID_USUARIO) b\n" +
                        "	ON a.id_usuario = b.id_usuario	\n" +
                        "ORDER BY 		MAX_FECHA_ASGINACION";
              
                        /*
                      
              String querylistatrabajo = "SELECT ID_QUEUE, ID_TOKEN, ID_PRIORIDAD,  FECHA_REGISTRO\n" +
                                "	FROM DG3861.PW_QUEUE_BITACTUAL\n" +
                                "	WHERE ESTATUS = 1\n" +
                                "	ORDER BY ID_PRIORIDAD, FECHA_REGISTRO ";

              
              String querycargatrabajo = "SELECT C.ID_USUARIO , nvl(a.TOTAL_ATENDIENDO,0) TOTAL_ATENDIENDO, \n" +
                "	nvl(b.MAX_FECHA_ASGINACION, sysdate) MAX_FECHA_ASGINACION, DQUEUE  FROM \n" +
                "PW_QUEUE_CRONO c\n" +
                "LEFT JOIN (\n" +
                "		SELECT ID_USUARIO, COUNT(1) TOTAL_ATENDIENDO FROM DG3861.PW_QUEUE_BITACTUAL \n" +
                "		WHERE ESTATUS IN (5, 3)\n" +
                "		AND ID_USUARIO IS NOT NULL\n" +
                "		GROUP BY ID_USUARIO\n" +
                "		) a\n" +
                "		ON a.id_usuario = c.id_usuario				\n" +
                "	LEFT JOIN (\n" +
                "		SELECT ID_USUARIO, MAX(FECHA_ASIGNACION) MAX_FECHA_ASGINACION FROM DG3861.PW_QUEUE_BITACTUAL \n" +
                "		--WHERE ESTATUS = 6\n" +
                "		WHERE ID_USUARIO IS NOT NULL\n" +
                "		GROUP BY ID_USUARIO) b\n" +
                "	ON a.id_usuario = b.id_usuario	\n" +
                "ORDER BY 		MAX_FECHA_ASGINACION";
               */
               
              Herramientas.Conexion con = new Herramientas.Conexion("DG3861_DGLDCSTG");
              ResultSet rSet;
              
              

               listatrabajo.clear();
               cargatrabajo.clear();
               
              // System.out.println(querylistatrabajo);
               
               rSet = con.ejecutarQuery(querylistatrabajo);
        
               while (rSet.next()) {
                      // System.out.println(rSet.getString(1)+","+ rSet.getString(2));
                       SetPlayTemis disp = new SetPlayTemis(rSet.getInt(1),rSet.getString(2),rSet.getInt(3),rSet.getString(4));
                       listatrabajo.add(disp);
                     
                   }
                 
             
               
         
              // System.out.println(querycargatrabajo);
                rSet = con.ejecutarQuery(querycargatrabajo);       
                while (rSet.next()) {
                      // System.out.println(rSet.getString(1)+","+ rSet.getString(2));
                       SetPlayTemis disp = new SetPlayTemis(rSet.getInt(1),rSet.getInt(2),rSet.getString(3),rSet.getInt(4), "");
                       cargatrabajo.add(disp);
                     
                   }
              
         
                
                      con.con.close();  
                     
                      
                      // 1 ---6 --- 5  
                      // 2 ---4 --- 5   
                      
                      //4 usuario ct 0 - 3
                        
                 int usuario=0;    
                 insertsfinales.clear();
                 for(int lt=0; lt<listatrabajo.size(); lt++)
                 {
                     //System.out.println("cargatrabajo.size()"+cargatrabajo.size());
                      boolean dispasingar = true;
                       for(int ct=usuario; ct<cargatrabajo.size(); ct++)
                                {
                                   
                                    if(dispasingar){
                                             System.out.println("lt:"+lt+" ct:"+ct+ "  dispasingar:"+dispasingar);
                                                System.out.println("usuario:"+cargatrabajo.get(ct).ID_USUARIO+" totalatendiendo:"+cargatrabajo.get(ct).TOTAL_ATENDIENDO+" queu:"+cargatrabajo.get(ct).DQUEUE+ "  cargatrabajo:"+cargatrabajo.size());
                                                
                                                            if (cargatrabajo.get(ct).TOTAL_ATENDIENDO < cargatrabajo.get(ct).DQUEUE ) 
                                                                { 
                                                                    
                                                                    System.out.print("Asigna al usuario:"+cargatrabajo.get(ct).ID_USUARIO+" id_token:"+listatrabajo.get(lt).ID_TOKEN);
                                                                    int newct=cargatrabajo.get(ct).TOTAL_ATENDIENDO +1;
                                                                    System.out.print("---->newct:"+newct);
                                                                    SetPlayTemis spct = new SetPlayTemis(cargatrabajo.get(ct).ID_USUARIO, newct, cargatrabajo.get(ct).MAX_FECHA_ASGINACION,cargatrabajo.get(ct).DQUEUE,"" );
                                                                    cargatrabajo.set(ct, spct);
                                                                   // System.out.println("---->resultado"+cargatrabajo.get(ct).ID_USUARIO+ "newct"+ cargatrabajo.get(ct).TOTAL_ATENDIENDO+ " maxfa"+cargatrabajo.get(ct).MAX_FECHA_ASGINACION+ " dqueu:"+cargatrabajo.get(ct).DQUEUE);
                                                                    dispasingar=false;
                                                                
                                                                    insertsfinales.add(
                                                                            "INSERT INTO DG3861.PW_QUEUE_BITACORA\n" +
                                                                            "(ID_TOKEN, TICKET_NUMBER, ID_USUARIO, ID_LOGIN, ESTATUS, FECHA_ASIGNACION, FECHA_ESTATUS, FECHA_CIERRE, ID_PRIORIDAD, BAND_REASIGNADO, FECHA_REGISTRO)\n" +
                                                                            "   VALUES ('"+listatrabajo.get(lt).ID_TOKEN+"', "
                                                                                    + "NULL, "
                                                                                    + ""+cargatrabajo.get(ct).ID_USUARIO+", "
                                                                                    + "NULL, "
                                                                                    + "3, sysdate, sysdate, NULL, "
                                                                                    + ""+listatrabajo.get(lt).ID_PRIORIDAD+", 0, sysdate)");
                                                                            
                                                                    
                                                                    
                                                                    
                                                                    
                                                                  
                                                                }
                                                              if (usuario == cargatrabajo.size()-1)
                                                                    {
                                                                        usuario =0;
                                                                    }
                                                                      else
                                                             {usuario++;}
                                    }                                                      
                                    
                                }                 
                 }
                 
                 
               con = new Herramientas.Conexion("DG3861_DGLDCSTG");
              
                      for(int i=0; i<insertsfinales.size(); i++)
                      {
                          System.out.println (insertsfinales.get(i).toString());
                          rSet = con.ejecutarQuery(insertsfinales.get(i).toString()); 
                      }
                       
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
                   
                    rSet = con.ejecutarQuery(querymrgbitactual);
                      
                 con.con.close();
              
        
        
          } catch (SQLException ex) {
              //System.out.println("Piscachas con los querys RMS QUEUE"+ex);
//              Logger.getLogger(Queu_Temis.class.getName()).log(Level.SEVERE, null, "Piscachas con los querys RMS QUEUE"+ex);
             
          }
        
          
          
      }
    
    
}
