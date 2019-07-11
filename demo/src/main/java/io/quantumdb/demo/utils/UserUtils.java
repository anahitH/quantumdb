package io.quantumdb.demo.utils;

import java.util.Random;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)

public class UserUtils {


    /**
     * Popular male names in Florence, Italy (1427): http://www.behindthename.com/top/lists/ih/1427
     */
    private static final String[] NAMES = {
            "Abramo", "Adamo", "Adovardo", "Agnolino", "Agnolo", "Agostino", "Alamanno", "Alberto", "Albizzo",
            "Alessandro", "Alesso", "Ambrogio", "Amerigo", "Amideo", "Andrea", "Anichino", "Antonio", "Apollonio",
            "Arrigo", "Attaviano", "Averardo", "Baldassarr", "Baldo", "Banco", "Bandino", "Bardo", "Barone", "Bartolo",
            "Bartolomeo", "Bastiano", "Battista", "Bencivenni", "Benedetto", "Benino", "Benozzo", "Benvenuto",
            "Bernaba", "Bernardo", "Bertino", "Berto", "Bettino", "Betto", "Biagio", "Bianco", "Bindo", "Boccaccio",
            "Bonacorso", "Bonaguida", "Bonaiuto", "Bonifazio", "Bonino", "Bono", "Bonsi", "Bruno", "Buccio", "Buono",
            "Buto", "Cambino", "Cambio", "Cardinale", "Carlo", "Castello", "Cecco", "Cenni", "Chiaro", "Chimenti",
            "Chimento", "Cino", "Cione", "Cipriano", "Cola", "Conte", "Corrado", "Corso", "Cosimo", "Cristofano",
            "Daddo", "Daniello", "Dego", "Deo", "Diedi", "Dino", "Doffo", "Domenico", "Donato", "Donnino", "Duccio",
            "Fabiano", "Fede", "Federigo", "Felice", "Feo", "Filippo", "Francesco", "Franco", "Frosino", "Gabbriello",
            "Gentile", "Geri", "Gherardino", "Gherardo", "Ghirigoro", "Giannino", "Giannozzo", "Giano", "Gino",
            "Giorgio", "Giovacchin", "Giovanni", "Giovannozz", "Giovenco", "Girolamo", "Giunta", "Giusto", "Goro",
            "Grazia", "Gualterott", "Gualtieri", "Guasparre", "Guccio", "Guelfo", "Guglielmo", "Guido", "Iacomo",
            "Iacopo", "Lamberto", "Lando", "Lapaccio", "Lapo", "Lazzero", "Leonardo", "Lippo", "Lodovico", "Lorenzo",
            "Lotto", "Luca", "Luigi", "Maccio", "Maffeo", "Mainardo", "Manetto", "Manno", "Marchionne", "Marco",
            "Mariano", "Marino", "Mariotto", "Martino", "Maso", "Matteo", "Meo", "Michele", "Migliore", "Miniato",
            "Mino", "Monte", "Naldo", "Nanni", "Nannino", "Nardo", "Nello", "Nencio", "Neri", "Niccola", "Niccolaio",
            "Niccolino", "Niccolo", "Nigi", "Nofri", "Nozzo", "Nuccio", "Nuto", "Orlando", "Ormanno", "Pace", "Pacino",
            "Pagolo", "Palla", "Pandolfo", "Papi", "Pasquino", "Piero", "Pierozzo", "Pietro", "Pippo", "Polito",
            "Priore", "Puccino", "Puccio", "Ramondo", "Riccardo", "Ricco", "Ridolfo", "Rinaldo", "Rinieri", "Ristoro",
            "Roberto", "Romigi", "Romolo", "Rosso", "Ruggieri", "Salvadore", "Salvestro", "Salvi", "Sandro", "Santi",
            "Scolaio", "Simone", "Sinibaldo", "Smeraldo", "Spinello", "Stagio", "Stefano", "Stoldo", "Strozza",
            "Taddeo", "Tano", "Tieri", "Tingo", "Tommaso", "Tomme", "Ubertino", "Uberto", "Ugo", "Ugolino", "Uguccione",
            "Urbano", "Vanni", "Vannozzo", "Ventura", "Vettorio", "Vico", "Vieri", "Vincenzo", "Zaccheria", "Zanobi"
    };

    public static String pickName() {
        Random random = new Random();
        return pickName(random);
    }

    public static String pickName(Random random) {
        int index = random.nextInt(NAMES.length);
        return NAMES[index];
    }

    public static String getEmail(String name) {
        return name.toLowerCase() + "@florence.it";
    }
}
