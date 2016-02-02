package Import::Util::Geo;

use Modern;

use JSON;
use LWP::UserAgent;

# Геокодирование 2GIS
sub get_coords_by_addr {
    my ($addr, $house_num) = @_;

    state $_geocache;

    my ($latitude, $longitude);
    my $q = $addr . ', ' . $house_num;

    my $ua = LWP::UserAgent->new;
    my $response = $ua->post(
        'http://catalog.api.2gis.ru/geo/search',
        [
            q => $q,
            key => 'rujrdp3400',
            version => '1.3',
            output => 'json',
            types => 'house',
        ],
        Referer => 'http://catalog.api.2gis.ru/',
    );
    if ($response->is_success) {
        eval {
            my $data = decode_json($response->decoded_content);
            return unless $data->{'total'};
            if (my $centroid = $data->{'result'}->[0]->{'centroid'}) {
                if ($centroid =~ /^POINT\((\d+\.\d+) (\d+\.\d+)\)$/) {
                    ($longitude, $latitude) = ($1, $2);
                    $_geocache->{$q} = [latitude => $latitude, longitude => $longitude];
                }
            }
            1;
        } or do {};
    } else {
        say "2GIS Invalid response (q: $q)";
    }

    return ($latitude && $longitude ? (latitude => $latitude, longitude => $longitude) : ());
}

1;
