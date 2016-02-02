package Import::Util::Mediator;

use Import::Modern;

use Import::Model::Mediator;
use Import::Model::Mediator::Manager;
use Import::Model::MediatorCompany;
use Import::Model::MediatorCompany::Manager;

use Exporter qw(import);
 
our @EXPORT_OK = qw(delete_mediator add_mediator remove_obsolete_mediators);

sub remove_obsolete_mediators {
    my $obs_period = shift;
    my $num_rows_updated = Import::Model::Mediator::Manager->update_objects(
        set => {delete_date => \'now()'},
        where => [
            [\"last_seen_date < (NOW() - INTERVAL '$obs_period day')"],
            delete_date => undef],
    );

    return $num_rows_updated;
}

sub delete_mediator {
    my $id = shift;

    my $num_rows_updated = Import::Model::Mediator::Manager->update_objects(
        set => {delete_date => \'now()'},
        where => [id => $id, delete_date => undef],
    );
}

sub add_mediator {

    # Prepare data
    my $company_name = shift;
    my $phone_num = shift;

	my $mediator;
	if (Import::Model::Mediator::Manager->get_objects_count(query => [phone_num => $phone_num, delete_date => undef])) {
		$mediator = Import::Model::Mediator::Manager->get_objects(query => [phone_num => $phone_num, delete_date => undef])->[0];
	} else {
		$mediator = Import::Model::Mediator->new;
	}

    $mediator->phone_num($phone_num);

    my $company = Import::Model::MediatorCompany::Manager->get_objects(query => [[\'lower(name) = ?' => lc($company_name)], delete_date => undef])->[0];
    unless ($company) {
        $company = Import::Model::MediatorCompany->new(name => $company_name);
        $company->save;
    }
    $mediator->company($company);
    $mediator->last_seen_date('now()');
    $mediator->save;

    # Search for additional mediator phones
    my $found_phones = Mojo::Collection->new();
    my $realty_iter = Import::Model::Realty::Manager->get_objects_iterator(query => [delete_date => undef, \("owner_phones && '{".$phone_num."}'")]);
    while (my $realty = $realty_iter->next) {
        push @$found_phones, ($realty->owner_phones);
    }
    $found_phones = $found_phones->uniq;

    if ($found_phones->size) {
        # Add additional mediators from realty owner phones
        for (@$found_phones) {
            if ($_ ne $phone_num && !Import::Model::Mediator::Manager->get_objects_count(query => [phone_num => $_, delete_date => undef])) {
                my $nm = Import::Model::Mediator->new(phone_num => $_, company => $company);
                $nm->save;
            }
        }
    }
}

sub check_mediators {
    my $mediator_iter = Rplus::Model::Mediator::Manager->get_objects_iterator(query => [delete_date => undef], require_objects => ['company']);
    while (my $x = $mediator_iter->next) {
        
        # Search for additional mediator phones
        my $found_phones = Mojo::Collection->new();
        my $realty_iter = Rplus::Model::Realty::Manager->get_objects_iterator(query => [delete_date => undef, \("owner_phones && '{".$x->phone_num."}'")]);
        while (my $realty = $realty_iter->next) {
            $realty->mediator($x->company->name);
            $realty->agent_id(10000);
            $realty->state_code('raw');
            $realty->save(changes_only => 1);
            say $realty->id;
            push @$found_phones, ($realty->owner_phones);
            #$self->realty_event('m', $realty->id);
        }
        $found_phones = $found_phones->uniq;

        if ($found_phones->size) {
            # Add additional mediators from realty owner phones
            for (@$found_phones) {
                if ($_ ne $x->phone_num && !Rplus::Model::Mediator::Manager->get_objects_count(query => [phone_num => $_, delete_date => undef])) {
                    say $_;
                    Rplus::Model::Mediator->new(phone_num => $_, company => $x->company->id)->save;
                }
            }
        }
    }
}

1;
