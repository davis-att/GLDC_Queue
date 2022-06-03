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
public class Queu_ActualizaLoguin_job implements Job{
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

      
          try {
              System.out.println("Actualiza Desconexion de LOGUIN");
               ResultSet rs;        

              String Query = "MERGE INTO DG3861.PW_QUEUE_LOGUIN  x\n" +
                            "		USING (\n" +
                            "				SELECT (sysdate - (nvl(a.FECHA_DESCONEXION,b.FECHA_FINAL))),a.*,b.FECHA_FINAL FROM DG3861.pw_queue_loguin a\n" +
                            "				INNER JOIN\n" +
                            "					(\n" +
                            "					  SELECT * FROM DG3861.PW_QUEUE_PROGRAMACION\n" +
                            "								WHERE (ID_USUARIO, FECHA_FINAL) IN\n" +
                            "										(SELECT ID_USUARIO, MAX(FECHA_FINAL) FROM DG3861.PW_QUEUE_PROGRAMACION GROUP BY ID_USUARIO)\n" +
                            "					) b\n" +
                            "				ON a.ID_USUARIO = b.ID_USUARIO\n" +
                            "				where FECHA_DESCONEXION IS NULL\n" +
                            "				AND (sysdate- 1/24) > (nvl(a.FECHA_DESCONEXION,b.FECHA_FINAL))\n" +
                            "			 ) y\n" +
                            "		ON (x.id_loguin = y.id_loguin )\n" +
                            "WHEN MATCHED THEN\n" +
                            "    UPDATE SET \n" +
                            "    x.FECHA_DESCONEXION = SYSDATE,\n" +
                            "    x.ID_DESCONEXION = 2 "; 
              

              Herramientas.Conexion con = new Herramientas.Conexion("DG3861_DGLDCSTG");
              
              rs = con.ejecutarQuery(Query);
               
              
              con.con.close();
              
          } catch (SQLException ex) {
              //System.out.println("Piscachas con los querys RMS QUEUE"+ex);
              Logger.getLogger(Queu_ActualizaLoguin_job.class.getName()).log(Level.SEVERE, null, "Piscachas con los querys RMS QUEUE"+ex);
             
          }
        
          
          
      }
    
    
}
