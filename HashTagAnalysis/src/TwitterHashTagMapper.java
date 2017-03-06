import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.String;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class TwitterHashTagMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	String[] field = line.split(";");
	if (field.length == 4) {
	String supportMsg = field[2].toLowerCase();
		if (supportMsg.startsWith("#")||supportMsg.startsWith("##")||supportMsg.startsWith("#medalfor")||supportMsg.startsWith("##team")||supportMsg.startsWith("#all")||supportMsg.startsWith("#allfor")||supportMsg.startsWith("#amoa")||supportMsg.startsWith("#brav")||supportMsg.startsWith("#bravo")||supportMsg.startsWith("#celebra")||supportMsg.startsWith("#comite")||supportMsg.startsWith("#congrats")||supportMsg.startsWith("#delegacion")||supportMsg.startsWith("#delegación")||supportMsg.startsWith("#fighting")||supportMsg.startsWith("#for")||supportMsg.startsWith("#get")||supportMsg.startsWith("#go")||supportMsg.startsWith("#goldfor")||supportMsg.startsWith("#goteam")||supportMsg.startsWith("#gracias")||supportMsg.startsWith("#imagesof")||supportMsg.startsWith("#imwith")||supportMsg.startsWith("#love")||supportMsg.startsWith("#proud")||supportMsg.startsWith("#my")||supportMsg.startsWith("#prayfor")||supportMsg.startsWith("#somos")||supportMsg.startsWith("#support")||supportMsg.startsWith("#supportteam")||supportMsg.startsWith("#team")||supportMsg.startsWith("#vamos")||supportMsg.startsWith("#vai")||supportMsg.startsWith("#us")||supportMsg.startsWith("#mon")||supportMsg.startsWith("#sol")||supportMsg.startsWith("#eri")){


		Pattern tagPattern = Pattern.compile("(americansamoa|austria|bahrain|belarus|burundi|belize|bosniaandherzegovina|cameroon|centralafricanrep|china|chinese|costarica|cookislands|iceland|iran|iraq|ireland|republicofkorea|malaysia|mali|mauritania|drcongo|congo|capeverde|saintkittsandnevis|saintlucia|dominica|southafrica|dprkorea|elsalvador|guam|libya|liberia|indonesia|mongolia|refugeeolympicteam|grenada|hongkong|maldives|malawi|mauritius|southsudan|montenegro|nigeria|malta|maltese|britishvirginislands|isv|usvirginislands|yem|zam|zim|bru|bur|prk|samoan|argentinian|argentine|belarus|motswana|costarica|ivorian|danish|congolese|elsalvador|equatorialguinea|french|alemania|greatbritain|uk|gb|british|britain|england|iceland|persian|irish|japan|kyrgyzstan|lebanon|lebanese|lithuania|malagasy|marshallislands|micronesia|burmese|morocc|nauru|netherlands|holland|dutch|kiwi|newzealand|nicaragua|palau|belau|pelew|palestine|papuanewguinea|pilipinas|filipino|filipina|pinoy|puertorico|boricua|korean|southkorean|rok|moldovan|romania|sanmarino|saotomeandprincipe|saudiarabia|serbia|sierraleone|slovakia|soomaaliya|spain|españa|spani|stvincandgrenadines|swaziland|sverige|switzerland|swiss|schweizer|suisse|tajikistan|taipei|chinesetaipei|siamese|macedonia|makedonija|theformeryugoslavrepublicofmacedonia|timorleste|tonga|trinidadandtobago|trinidadian|turkmenistan|unitedarabemirates|unitedstates|emirates|america|qatar|afg|alb|alg|asa|and|ang|ant|arg|arm|aru|aus|aut|aze|bah|brn|ban|bar|blr|bel|biz|ben|ber|bhu|bih|bot|bra|bul|bdi|cam|cmr|can|cpv|cay|caf|cha|chi|chn|col|com|cgo|cok|crc|cro|cub|cyp|cze|civ|den|dji|dma|dom|cod|ecu|egy|geq|eri|est|eth|fij|fin|fra|gab|gam|geo|ger|gha|gbr|gre|grn|gum|gua|gui|gbs|guy|hai|hon|hkg|hun|isl|ind|ina|ioa|iri|irq|irl|isr|ita|jam|jpn|jor|kaz|ken|kir|kos|kuw|kgz|lao|lat|lib|les|lbr|lba|lie|ltu|lux|mad|maw|mas|mdv|mli|mlt|mhl|mtn|mri|mex|fsm|mon|mgl|mne|mar|moz|mya|nam|nru|nep|ned|nzl|nca|nig|ngr|nor|oma|pak|plw|ple|pan|png|par|per|phi|pol|por|pur|rot|kor|mda|rou|rus|rwa|skn|lca|sam|smr|stp|ksa|sen|srb|sey|sle|sin|svk|slo|sol|som|rsa|ssd|qat|esp|sri|vin|sud|sur|swz|swe|sui|syr|tpe|tjk|tan|tha|mkd|tls|tog|tga|tto|tun|tur|tkm|tuv|uga|ukr|uae|usa|uru|uzb|van|ven|vie|ivb|tw)$");


				Matcher tagMatcher = tagPattern.matcher(supportMsg);
					while (tagMatcher.find()){			
				data.set(tagMatcher.group().toString().replaceAll("[^a-zA-Z]", ""));
				context.write(data, one);
					}
			}
		}		
	}
}


