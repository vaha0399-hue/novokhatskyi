import { FAAuthExperience } from "@/components/fa-auth-experience";
import { safeNextPath } from "@/lib/routes";
import { authPageMessage } from "@/lib/supabase/auth-messages";

export default async function LoginPage({ searchParams }: { searchParams: Promise<{ next?: string; error?: string }> }) {
  const parameters = await searchParams;
  const nextPath = safeNextPath(parameters.next);
  return <FAAuthExperience initialView="signin" nextPath={nextPath} initialMessage={authPageMessage(parameters.error)} />;
}
