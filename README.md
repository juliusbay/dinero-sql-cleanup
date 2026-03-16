**💼 Dinero Oprydning**
Denne guide beskriver trin-for-trin processen for oprydning i Dinero-data.

Scriptet læser en kundeliste fra en CSV-fil og identificerer dubletter baseret på kundens navn og vejnavn. Formålet er at skabe en entydig reference mellem dublette kunder og en fælles “kanonisk” post. 
Scriptet indlæser først data og normaliserer kolonnenavne ved at fjerne mellemrum, anførselstegn og gøre dem små. Det kontrollerer, at de nødvendige kolonner findes, og konverterer id’er og kundenumre til tal.

Derefter tilføjes to hjælpekolonner med normaliseret navn og vejnavn (i små bogstaver uden mellemrum). Disse bruges til at gruppere kunder, så poster med samme navn og adresse behandles sammen. 
I hver gruppe sorteres kunderne efter deres id, og den række, der har det laveste id, betragtes som den “kanoniske” kunde. Den beholder sit eksisterende dinero_handle, eller får standardværdien “unik”, hvis feltet er tomt. 
Øvrige kunder i gruppen får det samme kanoniske id og kundenummer, men uden dinero_handle.

Til sidst konstrueres en mapping-fil, der viser forholdet mellem hver kunde og dens kanoniske post. 
Den originale customer_number bevares, og outputtet indeholder både de oprindelige og kanoniske id’er samt handles. 
Rækkerne sorteres, så den kanoniske kunde altid står øverst i hver gruppe, hvorefter resultatet gemmes som customer_mapping.csv. 
Filen kan derefter bruges til at slå dubletter sammen eller synkronisere data med andre systemer som for eksempel Dinero.

**🔧 Trin-for-trin guide**
_1. Opret en backup i SQL Server Management Studio.

2. Kør kundesynkronisering via Action Board.

3. Eksporter følgende til Excel:
    Ordrer
    Kundefakturaer
    Leverandører
    Leverandørfakturaer_

_4. Lav et udtræk af kunder fra databasen og gem filen i script-mappen med navnet 'customers'.

5. Kør scriptet fra scriptmappen (python3 dinero_mapping_temp_table.py)
   
6. Opret en ny tabel i databasen kaldet dinero_temp_cleanup.
    
7. Importer output-fil til ny tabel
    
8. Husk at tilføj flueben ved “Set empty strings to NULL” under import._

_9. Map dinero handles ud fra id:_
    
    UPDATE cc
    SET cc.dinero_handle = dtc.dinero_handle
    FROM cust_customer AS cc
    INNER JOIN dinero_temp_cleanup AS dtc
    ON cc.id = dtc.customer_id
    
_10. Slet rows uden dinero_handle i employee_type_customers:_

      DELETE etc
      FROM employee_type_customer etc
      INNER JOIN cust_customer c
      ON etc.customer_id = c.id
      WHERE c.dinero_handle IS NULL

_11. Opdater customer-id i cases/offers/customer_comments/assets/cust_delivery_addresses (du kan bare erstatte tabel-navnet nede i statementet):_

    UPDATE c
    SET c.customer_id = dtc.canonical_customer_id
    FROM cases AS c
    INNER JOIN dinero_temp_cleanup AS dtc
    ON c.customer_id = dtc.customer_id

_12. Map kundenumre til canonical kundenummer i invoice:_

    UPDATE c
    SET c.customer_number = dtc.canonical_customer_number
    FROM invoice AS c
    INNER JOIN dinero_temp_cleanup AS dtc
    ON c.customer_number = dtc.customer_number

_13. Map leverandørnumre til canonical leverandørnummer i creditor_invoice_header:_

    UPDATE c
    SET c.creditor_number = dtc.canonical_customer_number
    FROM creditor_invoice_header AS c
    INNER JOIN dinero_temp_cleanup AS dtc
    ON c.creditor_number = dtc.customer_number

_14. Slet kunder med dinero handle NULL_
        
      DELETE FROM cust_customer
      WHERE dinero_handle IS NULL

_15.	Opdater dinero_handle ‘unik’ til NULL_

      UPDATE cust_customer
      SET dinero_handle = NULL
      WHERE dinero_handle = ’unik’

_16. Lav nyt udtræk af ordrer og fakturaer og sæt før-filen ved siden af efter-filen_

_17. Sortér på id/ordrenummer/fakturanummer fra lavest til højest i begge filer, så de er sorteret ens_

_18. Kopier navne fra før-fil til efter-fil i en ny kolonne og sammenlign med nedenstående formel_

_19. Lav ny kolonne og tjek for afvigelser med nedenstående formel. Alle skal være OK, evt. afvigelser burde kun skyldes mellemrum i navne. Juster formel efter før- og efter-kolonners placering_
    
  = IF(B2 = C2; "OK"; "AFVIGELSE")




