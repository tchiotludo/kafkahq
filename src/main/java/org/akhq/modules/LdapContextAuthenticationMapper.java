package org.akhq.modules;

import io.micronaut.configuration.security.ldap.ContextAuthenticationMapper;
import io.micronaut.configuration.security.ldap.DefaultContextAuthenticationMapper;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.convert.value.ConvertibleValues;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UserDetails;
import org.akhq.configs.Ldap;
import org.akhq.utils.ClaimProvider;
import org.akhq.utils.UserGroupUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Set;

@Singleton
@Replaces(DefaultContextAuthenticationMapper.class)
public class LdapContextAuthenticationMapper implements ContextAuthenticationMapper {

    @Inject
    private ClaimProvider claimProvider;

    @Override
    public AuthenticationResponse map(ConvertibleValues<Object> attributes, String username, Set<String> groups) {
        ClaimProvider.AKHQClaimRequest request = ClaimProvider.AKHQClaimRequest.builder()
                .providerType(ClaimProvider.ProviderType.LDAP)
                .providerName(null)
                .username(username)
                .groups(List.copyOf(groups))
                .build();

        ClaimProvider.AKHQClaimResponse claim = claimProvider.generateClaim(request);

        return new UserDetails(username, claim.getRoles(), claim.getAttributes());
    }


}
