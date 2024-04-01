select 

    *,
    case
        when source = '(direct)' then 'Direct'
        when source = 'google' and medium = 'organic' then 'SEO'
        when source = 'google' and medium = 'cpc' and campaign like 'Search%Brand%' then 'SEA Brand'
        when source = 'google' and medium = 'cpc' and campaign like 'Search%' then 'SEA Generic'
        when source = 'fb' and medium = 'paid' then 'Paid Social'
        when source = 'facebook' and medium = 'referral' then 'Organic Social'
        when medium = 'referral' then 'Referral'
        else 'Unknown'
    end marketing_channel

from {{ ref("source_traffic") }}