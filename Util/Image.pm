package Import::Util::Image;

use Import::Modern;

use Import::Model::Photo;
use Import::Model::Photo::Manager;

use Import::Util::Config;

use Time::HiRes;
use File::Path qw(make_path);
use Image::Magick;

sub load_image {
    my ($realty_id, $file, $storage_path, $crop) = @_;

    my $c_crop = 50;

    my $path = $storage_path.'/photos/'.$realty_id;
    my $name = Time::HiRes::time =~ s/\.//r; # Unique name

    my $photo = Import::Model::Photo->new;

    make_path($path);
    $file->move_to($path.'/'.$name.'.jpg');

    # Convert image to jpeg
    my $image = Image::Magick->new;
    $image->Read($path.'/'.$name.'.jpg');
    if ($c_crop != 0 && $c_crop < $image->Get('height')) {
        my $ht = $image->Get('height') - $c_crop;
        my $wd = $image->Get('width');
        $image->Extent(geometry => $wd.'x'.$ht, gravity => 'North', background => 'white');
        #$image->Chop(gravity => 'South', geometry => '0x'.$crop);
    }
    if ($image->Get('width') > 960 || $image->Get('height') > 540 || $image->Get('mime') ne 'image/jpeg') {
        $image->Resize(geometry => '960x540');
    }
    $image->Write($path.'/'.$name.'.jpg');
    $image->Resize(geometry => '320x240');
    $image->Extent(geometry => '320x240', gravity => 'Center', background => 'white');
    $image->Thumbnail(geometry => '320x240');
    $image->Write($path.'/'.$name.'_thumbnail.jpg');

    # Save
    $photo->realty_id($realty_id);
    $photo->filename($name.'.jpg');
    $photo->thumbnail_filename($name.'_thumbnail.jpg');

    $photo->save;

    # Update realty change_date
    Import::Model::Realty::Manager->update_objects(
        set => {change_date => \'now()'},
        where => [id => $realty_id],
    );
}

1;
