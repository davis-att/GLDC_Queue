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
 * @author dg3861&rr789j&test
 */
public class Queu_ActualizaCrono_job implements Job {
    
   @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

          
          try {
              System.out.println("Actualización Cronos");
              
              
              String querytruncate = "TRUNCATE TABLE DG3861.PW_QUEUE_CRONO";
              String queryinsert = "INSERT INTO DG3861.PW_QUEUE_CRONO \n" +
                                        "with qusuariod as \n" +
                                        "(\n" +
                                        "select a.*, b.id_loguin, b.id_sesion, b.fecha_loguin, b.fecha_desconexion, b.id_desconexion \n" +
                                        "	from dg3861.pw_queue_programacion a \n" +
                                        "		inner join dg3861.pw_queue_loguin b \n" +
                                        "			on a.id_usuario = b.id_usuario \n" +
                                        "		where sysdate between a.fecha_inicial and a.fecha_final \n" +
                                        "		and sysdate not between a.hora_comida_inicio and a.hora_comida_fin \n" +
                                        "		and b.fecha_desconexion is null\n" +
                                        ") \n" +
                                        "select a.*, b.fecha_solicitud , b.fecha_reconexion, b.id_tiempod,\n" +
                                        " case \n" +
                                        " 	when sysdate between (a.fecha_final - (60/24/60)) and (a.fecha_final - (30/24/60)) then c.queue_salida\n" +
                                        " 	when sysdate between (a.hora_comida_inicio - (60/24/60)) and (a.hora_comida_inicio - (30/24/60)) then c.queue_comida\n" +
                                        " 	when sysdate between (a.fecha_final - (30/24/60)) and a.fecha_final  then 0\n" +
                                        " 	when sysdate between (a.hora_comida_inicio - (30/24/60)) and a.hora_comida_inicio  then 0\n" +
                                        " 	else c.max_queue \n" +
                                        " 	end dqueue, SYSDATE fecha_creacion\n" +
                                        "	from qusuariod a\n" +
                                        "		left join (\n" +
                                        "					select a.*, fecha_solicitud, (c.fecha_solicitud + (total_tiempo/24/60)) fecha_reconexion, id_tiempod from qusuariod a\n" +
                                        "					left join dg3861.pw_queue_tiempofuera c\n" +
                                        "						on a.id_usuario = c.id_usuario\n" +
                                        "						and sysdate between c.fecha_solicitud and (c.fecha_solicitud + (total_tiempo/24/60))\n" +
                                        "						and c.fecha_solicitud between a.fecha_inicial and a.fecha_final\n" +
                                        "					) b\n" +
                                        "			on a.id_usuario = b.id_usuario\n" +
                                        "		left join dg3861.pw_queue_usuario c\n" +
                                        "		 	on a.id_usuario = c.id_usuario  \n" +
                                        "where b.id_tiempod is NULL";
              //Trae información de LOS USUARIOS  DISPONIBLES.  
              Herramientas.Conexion con = new Herramientas.Conexion("DG3861_DGLDCSTG");
              ResultSet rs;
              
              rs = con.ejecutarQuery(querytruncate);
              rs = con.ejecutarQuery(queryinsert);

              
              con.con.close();
          } catch (SQLException ex) {
              //System.out.println("Piscachas con los querys RMS QUEUE"+ex);
              Logger.getLogger(Queu_ActualizaCrono_job.class.getName()).log(Level.SEVERE, null, "Piscachas con los querys RMS QUEUE"+ex);
             
          }
        
          
          
      }
    
    
}
