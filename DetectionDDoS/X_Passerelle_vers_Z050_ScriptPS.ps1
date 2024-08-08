#compte et mot de passe 
#$user='AD\dg57-cei-svc-supervi'
#$Password='UHf+hukpJ1!phq*epzoNgeQ'

# mot de passse visible sur Centreon via le service LDAP-Login et la machine pdaddcwpdwd101
$user='AD\PD0-SUPERVISION-SVC'
#$Password='WCs*5Zd@nlOrqz4'
#$Password='4RFVcde\"2ZSXwqa&'
#$Password='9SqV*PZ6cKYS#QHx0t#Q'
#$Password='dzW^Wdq6O6LqeE+pcVrr'
#$Password='3ln6qD@haqwuGIiA*AW3'
#$Password='c0T-TYzCKfQ^38zETfKN'
$Password='8*wAjIGoa@uGsd@ASOhF'


#nom du répertoire à copier
#$nom="E:\X_Passerelle_vers_Z050\*"  
$nom="C:\Users\siar_ycg8l6\ProgR\X_Passerelle_vers_Z050\*"


#montage du partage
#net use B: \\10.203.0.29\X_Passerelle_depuis_Z100 /USER:$user $Password /PERSISTENT:NO
#net use B: \\10.203.0.29\X_Passerelle_depuis_Z100 /USER:$user $Password /PERSISTENT:NO
net use B: \\10.203.0.29\Users\SIAR_ycg8l6\Docs\ProgrammesR\X_Passerelle_depuis_Z100 /USER:$user $Password /PERSISTENT:NO

#copie vers le partage
cp $nom B:

#demonte le partage
net use B: /DELETE /YES