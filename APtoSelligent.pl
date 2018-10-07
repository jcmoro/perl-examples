#!/usr/bin/perl -W
# Genera CSV para selligent con información de bbdd areausuario - favoritos, dispositivos, sorteos, cupones, tarjetas

        use DBI;

        my $ddb   = "xxxx"; # PRO
        my $dhost  = "localhost"; # LOCAL
	      my $duser = "test"; # LOCAL
        my $dpass  = ''; # LOCAL
        my $path  = "/home/user/test/";

        my $file  = "AREAUSUARIOS_%MEDIO%_%FECHA%_%TIMESTAMP%.csv";

        my $ftphost = "xxxxxxxxxx";
        my $ftpuser = "xxxxxxxxxxx";
        my $ftppasw = 'xxxxxxxxxx';
        my $ftpport = "38990";
        my $ftppath = "/IN/";
        my $dconn;

        my %medios;

        $medios{elcorreo}          = "('El Correo')";
        $medios{diariovasco}       = "('DiarioVasco')";
        $medios{elcomercio}        = "('El Comercio')";
        $medios{diariosur}         = "('DiarioSur')";
        $medios{eldiariomontanes}  = "('El Diario Montanez')";
        $medios{hoy}               = "('Hoy')";
        $medios{elnortedecastilla} = "('El Norte de Castilla')";
        $medios{ideal}             = "('Ideal')";
        $medios{larioja}           = "('La Rioja')";
        $medios{lasprovincias}     = "('Las Provincias')";
        $medios{laverdad}          = "('La Verdad')";
        $medios{leonoticias}       = "('Leonoticias')";

        if ($#ARGV eq -1) {
                die("Tiene que pasarse el parametro MEDIO\n");
        }

        my $medio = $ARGV[0];

        my $query = "SELECT UID,
                     	SUM(articulo_h) FAVORITO_H,
		  	MAX(fecha_fav) FAVORITO_U,
			SUM(dispositivo_h) DISPOSITIVO_H,
  			MAX(fecha_dis) DISPOSITIVO_U,
			SUM(sorteo_h) SORTEO_H,
  			MAX(fecha_sorteo30) SORTEO_1M,
  			MAX(fecha_sorteo90) SORTEO_3M,
 			MAX(fecha_sorteo180) SORTEO_6M,
  			SUM(cupon_h) CUPON_H,
  			MAX(fecha_cupon30) CUPON_1M,
  			MAX(fecha_cupon90) CUPON_3M,
  			MAX(fecha_cupon180) CUPON_6M,
  			SUM(tarjeta_h) TARJETA_H,
			MAX(fecha_tarjeta) TARJETA_U,
			MAX(idsorteo) SORTEOS_PARTICIPACIONES,
            MAX(ganadorSorteo) SORTEOS_GANADOR
    		FROM (
    			SELECT u1.token UID, u1.id_usuario usuario, count(f.id_articulo) articulo_h, MAX(f.fecha) fecha_fav, 0 dispositivo_h, NULL fecha_dis,
			0 sorteo_h, 0 cupon_h, 0 tarjeta_h, 0 fecha_sorteo30, 0 fecha_sorteo90, 0 fecha_sorteo180,
			0 fecha_cupon30, 0 fecha_cupon90, 0 fecha_cupon180, NULL fecha_tarjeta, null idsorteo, null ganadorSorteo
        		FROM usuario u1
        		    INNER JOIN ge_medio g1 ON g1.id_medio = u1.id_medio
			    LEFT JOIN articulo_fav f ON u1.id_usuario = f.id_usuario
        		WHERE
        		    g1.txt_dominio_id = '" . $medio . "'
        		    AND f.url != ''
        		    AND f.fecha IS NOT NULL
        		GROUP BY usuario
    		    UNION ALL
    			SELECT u2.token UID, u2.id_usuario usuario, 0 articulo_h, NULL fecha_fav, count(d.id_dispositivo) dispositivo_h, MAX(d.fechaAlta) fecha_dis,
	    		0 sorteo_h, 0 cupon_h, 0 tarjeta_h, 0 fecha_sorteo30, 0 fecha_sorteo90, 0 fecha_sorteo180,
			0 fecha_cupon30, 0 fecha_cupon90, 0 fecha_cupon180, NULL fecha_tarjeta, null idsorteo, null ganadorSorteo
        		FROM usuario u2
        		    INNER JOIN ge_medio g2 ON g2.id_medio = u2.id_medio
        		    LEFT JOIN dispositivo d ON u2.id_usuario = d.id_usuario
        		WHERE
        		    g2.txt_dominio_id = '" . $medio . "'
        		GROUP BY usuario
    		    UNION ALL
    			SELECT u3.token UID, u3.id_usuario usuario, 0 articulo_h, NULL fecha_fav, 0 dispositivo_h, NULL fecha_dis, count(rs.id_sorteo)
			sorteo_h, 0 cupon_h, 0 tarjeta_h,
     			SUM(CASE WHEN DATEDIFF(rs.fecha_alta, now()) > -30 AND DATEDIFF(rs.fecha_alta, now())  <= 0 THEN 1 END) fecha_sorteo30,
     			SUM(CASE WHEN DATEDIFF(rs.fecha_alta, now()) > -90 AND DATEDIFF(rs.fecha_alta, now())  <= 0 THEN 1 END) fecha_sorteo90,
     			SUM(CASE WHEN DATEDIFF(rs.fecha_alta, now()) > -180 AND DATEDIFF(rs.fecha_alta, now()) <= 0 THEN 1 END) fecha_sorteo180,
     			0 fecha_cupon30, 0 fecha_cupon90, 0 fecha_cupon180, NULL fecha_tarjeta, null idsorteo, null ganadorSorteo
        		FROM usuario u3
        		    INNER JOIN ge_medio g3 ON g3.id_medio = u3.id_medio
        		    LEFT JOIN rel_sorteo_usuario rs ON u3.id_usuario = rs.id_usuario
        		WHERE
        		    g3.txt_dominio_id = '" . $medio . "'
        		GROUP BY usuario
    		    UNION ALL
    			SELECT u4.token UID, u4.id_usuario usuario, 0 articulo_h, NULL fecha_fav, 0 dispositivo_h, NULL fecha_dis,
			0 sorteo_h, count(rc.id_cupon) cupon_h, 0 tarjeta_h,
     			0 fecha_sorteo30, 0 fecha_sorteo90, 0 fecha_sorteo180,
     			SUM(CASE WHEN DATEDIFF(rc.fecha_alta, now()) > -30 AND DATEDIFF(rc.fecha_alta, now())  <= 0 THEN 1 END) fecha_cupon30,
     			SUM(CASE WHEN DATEDIFF(rc.fecha_alta, now()) > -90 AND DATEDIFF(rc.fecha_alta, now())  <= 0 THEN 1 END) fecha_cupon90,
     			SUM(CASE WHEN DATEDIFF(rc.fecha_alta, now()) > -180 AND DATEDIFF(rc.fecha_alta, now()) <= 0 THEN 1 END) fecha_cupon180,
                        NULL fecha_tarjeta, null idsorteo, null ganadorSorteo
        		FROM usuario u4
          		    INNER JOIN ge_medio g4 ON g4.id_medio = u4.id_medio
        		    LEFT JOIN rel_cupon_usuario rc ON u4.id_usuario = rc.id_usuario
        		WHERE
        		    g4.txt_dominio_id = '" . $medio . "'
        		GROUP BY usuario
    		    UNION ALL
			SELECT u5.token UID, u5.id_usuario usuario, 0 articulo_h, NULL fecha_fav, 0 dispositivo_h, NULL fecha_dis,
			0 sorteo_h, 0 cupon_h, COUNT(tc.id) tarjeta_h, 0 fecha_sorteo30, 0 fecha_sorteo90, 0 fecha_sorteo180,
			0 fecha_cupon30, 0 fecha_cupon90, 0 fecha_cupon180, MAX(tc.fecha_solicitud) fecha_tarjeta, null idsorteo, null ganadorSorteo
        		FROM usuario u5
        		    INNER JOIN ge_medio g5 ON g5.id_medio = u5.id_medio
        		    LEFT JOIN tarjeta_club tc ON u5.id_usuario = tc.id_usuario
        		WHERE
        		    g5.txt_dominio_id = '" . $medio . "'
        		GROUP BY usuario
                    UNION ALL
                        SELECT u6.token UID, u6.id_usuario usuario, 0 articulo_h, NULL fecha_fav, 0 dispositivo_h, NULL fecha_dis,
                        0 sorteo_h, 0 cupon_h, 0 tarjeta_h, 0 fecha_sorteo30, 0 fecha_sorteo90, 0 fecha_sorteo180,
                        0 fecha_cupon30, 0 fecha_cupon90, 0 fecha_cupon180, null fecha_tarjeta, CONCAT('|', GROUP_CONCAT(rs2.id_sorteo separator '|'), '|') idsorteo, null ganadorSorteo
                        FROM usuario u6
                            INNER JOIN ge_medio g6 ON g6.id_medio = u6.id_medio
                            INNER JOIN rel_sorteo_usuario rs2 ON u6.id_usuario = rs2.id_usuario
                        WHERE
                            g6.txt_dominio_id = '" . $medio . "'
                        GROUP BY usuario
                    UNION ALL
                        SELECT u7.token UID, u7.id_usuario usuario, 0 articulo_h, NULL fecha_fav, 0 dispositivo_h, NULL fecha_dis,
                        0 sorteo_h, 0 cupon_h, 0 tarjeta_h, 0 fecha_sorteo30, 0 fecha_sorteo90, 0 fecha_sorteo180,
                        0 fecha_cupon30, 0 fecha_cupon90, 0 fecha_cupon180, null fecha_tarjeta, null idsorteo, CONCAT('|', GROUP_CONCAT(rs3.id_sorteo separator '|'), '|') ganadorSorteo
                        FROM usuario u7
                            INNER JOIN ge_medio g7 ON g7.id_medio = u7.id_medio
                            INNER JOIN rel_sorteo_usuario rs3 ON u7.id_usuario = rs3.id_usuario
                        WHERE
                            g7.txt_dominio_id = '" . $medio . "'
                            AND rs3.ganador = 1
                        GROUP BY usuario
    		) t_union GROUP BY usuario;";


        # conecta dbs
        sub connDB() {
                my $ddsn = "dbi:mysql:database=$ddb;host=$dhost";

                $dconn = DBI->connect($ddsn,$duser,$dpass);

                if (!$dconn) {
                        die("NO SE PUEDE CONECTAR CON LA DBS\n");
                }
                return(1);
        }

        # desconecta dbs
        sub deConn() {
                $dconn->disconnect;
                return(1);
        }

        # fechas
        sub getDate() {
                my($sec,$min,$hour,$mday,$mon,
                                $year,$wday,$yday,$isdst) = localtime(time());
                my %gD;

                $gD{y} = sprintf("%02d",$year+1900);
                $gD{M} = sprintf("%02d",$mon+1);
                $gD{d} = sprintf("%02d",$mday);
                $gD{h} = sprintf("%02d",$hour);
                $gD{m} = sprintf("%02d",$min);
                $gD{s} = sprintf("%02d",$sec);
                return %gD;
        }

        # cambia plantilla
        sub templates($) {
                my $medio = shift;
                my %f = getDate();
                my $fecha = $f{y}.$f{M}.$f{d};
                my $stamp = time();

                if (!$medios{$medio}) {
                        die("El MEDIO pasado no es correcto\nMedios validos:elcorreo, elcomercio, diariosur, diariovasco, eldiariomontanes, hoy, elnortedecastilla, ideal, larioja, lasprovincias, laverdad, leonoticias\n");
                }

		my $medio_uc = uc $medio;
                $file =~ s/%MEDIO%/$medio_uc/;
                $file =~ s/%FECHA%/$fecha/;
                $file =~ s/%TIMESTAMP%/$stamp/;
        }

        # fichero
        sub creafile() {
                open($file, ">$path/$file");
                return $file;
        }

        # ftp
        sub envia() {
                my $comm = "curl -s -k --retry 5 --retry-delay 30 ";
                $comm .= "--ftp-ssl-reqd --ftp-pasv --disable-epsv -T ".$path."/".$file;
                $comm .= " ftps://".$ftpuser.":".$ftppasw."@".$ftphost.":".$ftpport.$ftppath;
                my @res = `$comm`;
                if($? ne 0) {
                        die("No se ha podido hacer el envio a selligent\n");
                }
                unlink($path."/".$file);
        }

        # inicio

        templates($medio);
        my $file_desc = creafile();
        connDB();

        $dquery = $dconn->prepare($query);
        $dquery->execute;

        print $file_desc join(', ', @{$dquery->{NAME}})."\n";
        while (@drow = $dquery->fetchrow_array) {
                my $dvalues = "";
		my $v = "";
                foreach $v (@drow) {
			if($v || defined $v) {
                        	$dvalues .= "$v,";
			} else {
				$dvalues .= ",";
                        }
                }
                chop($dvalues);
                print $file_desc $dvalues."\n";
        }

        close($file_desc);
        deConn();
        envia();
